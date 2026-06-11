"""Tests for the SDK security gates (sdk_security).

The PreToolUse hooks and can_use_tool callback are async and require the
SDK types only at registration time (make_pre_tool_hooks); the underlying
gate *logic* is synchronous and SDK-independent, so we test it directly:
  - _resolve_inside  (safe_path containment: invariants #1/#2/#9/#35)
  - bash_command_allowed (argv allowlist: invariant #3)
  - scrubbed_env (env_scrub passthrough: invariant #5)
These mirror scripts/engineer_bot/tools.py + env_scrub.py semantics.
"""
import asyncio
from pathlib import Path

from scripts.shared import sdk_security


# ── safe_path containment (_resolve_inside) ──

def test_inside_root_allowed(tmp_path):
    assert sdk_security._resolve_inside(tmp_path, (), "tests/foo.cs") is not None


def test_parent_traversal_denied(tmp_path):
    assert sdk_security._resolve_inside(tmp_path, (), "../escape.txt") is None


def test_absolute_path_outside_denied(tmp_path):
    assert sdk_security._resolve_inside(tmp_path, (), "/etc/passwd") is None


def test_denied_subpath_directory(tmp_path):
    denied = (tmp_path / "driver",)
    assert sdk_security._resolve_inside(tmp_path, denied, "driver/secret.cs") is None
    assert sdk_security._resolve_inside(tmp_path, denied, "tests/ok.cs") is not None


def test_denied_subpath_exact_file(tmp_path):
    """A file entry in denied_subpaths denies exactly that file
    (is_relative_to is True for equal paths)."""
    denied = (tmp_path / ".gitleaksignore",)
    assert sdk_security._resolve_inside(tmp_path, denied, ".gitleaksignore") is None
    assert sdk_security._resolve_inside(tmp_path, denied, "other") is not None


def test_symlink_escape_denied(tmp_path):
    outside = tmp_path.parent / "outside_target"
    outside.mkdir(exist_ok=True)
    link = tmp_path / "link"
    link.symlink_to(outside)
    # resolve() follows the symlink; the target is outside the root.
    assert sdk_security._resolve_inside(tmp_path, (), "link/file.txt") is None


# ── bash argv allowlist ──

_ALLOW = (("dotnet", "build"), ("dotnet", "test"), ("git", "diff", "HEAD"))


def test_bash_allowed_prefix():
    assert sdk_security.bash_command_allowed("dotnet build MyProj.csproj", _ALLOW)
    assert sdk_security.bash_command_allowed("git diff HEAD", _ALLOW)


def test_bash_denied_not_in_list():
    assert not sdk_security.bash_command_allowed("cat /etc/passwd", _ALLOW)
    assert not sdk_security.bash_command_allowed("ls -la", _ALLOW)


def test_bash_denied_subcommand_mismatch():
    """('dotnet','build') must NOT permit `dotnet tool install`."""
    assert not sdk_security.bash_command_allowed("dotnet tool install -g evil", _ALLOW)


def test_bash_empty_and_unparseable_denied():
    assert not sdk_security.bash_command_allowed("", _ALLOW)
    assert not sdk_security.bash_command_allowed('git diff "unterminated', _ALLOW)


# ── env scrub (delegates to env_scrub.scrub, verbatim table) ──

def test_scrub_strips_credentials():
    src = {
        "DATABRICKS_TOKEN": "secret",
        "GITHUB_APP_PRIVATE_KEY": "k",
        "MY_API_KEY": "k",
        "AWS_ACCESS_KEY_ID": "k",
        "SOME_PASSWORD": "p",
    }
    out = sdk_security.scrubbed_env(src)
    assert out == {}


def test_scrub_keeps_non_credential_config():
    """The deliberate no-blanket-prefix rule: DATABRICKS_TEST_CONFIG_FILE
    must survive (the bash tool's dotnet-test invocation needs it)."""
    src = {
        "DATABRICKS_TEST_CONFIG_FILE": "/cfg.json",
        "DATABRICKS_TOKEN": "secret",
        "PATH": "/usr/bin",
    }
    out = sdk_security.scrubbed_env(src)
    assert out == {"DATABRICKS_TEST_CONFIG_FILE": "/cfg.json", "PATH": "/usr/bin"}


# ── async hook smoke (logic only; no SDK runtime required) ──

def test_safe_path_hook_denies_escape(tmp_path):
    hook = sdk_security.make_safe_path_hook(tmp_path, ())
    out = asyncio.run(hook(
        {"tool_name": "Read", "tool_input": {"file_path": "../../etc/passwd"}},
        "tid", None,
    ))
    assert out["hookSpecificOutput"]["permissionDecision"] == "deny"


def test_safe_path_hook_allows_inside(tmp_path):
    hook = sdk_security.make_safe_path_hook(tmp_path, ())
    out = asyncio.run(hook(
        {"tool_name": "Read", "tool_input": {"file_path": "tests/a.cs"}},
        "tid", None,
    ))
    assert out == {}  # empty == allow


def test_bash_hook_denies_and_clamps(tmp_path):
    hook = sdk_security.make_bash_allowlist_hook(_ALLOW, timeout=900)
    denied = asyncio.run(hook(
        {"tool_name": "Bash", "tool_input": {"command": "rm -rf /"}}, "tid", None,
    ))
    assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"
    allowed = asyncio.run(hook(
        {"tool_name": "Bash", "tool_input": {"command": "dotnet build"}}, "tid", None,
    ))
    assert allowed["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert allowed["tool_input"]["timeout"] == 900 * 1000  # clamped to ms


def test_can_use_tool_enforces_path_and_bash_for_mcp_names(tmp_path):
    """make_can_use_tool must apply safe_path + bash allowlist to MCP-qualified
    @tool names (mcp__engineer-tools__edit_file / __bash), not just built-ins.
    (Review #445 [7]/[8])"""
    cb = sdk_security.make_can_use_tool(
        tmp_path, denied_subpaths=(), bash_allowlist=(("git", "status"),),
        allowed_tool_names=("mcp__engineer-tools__edit_file", "mcp__engineer-tools__bash"),
    )
    deny = asyncio.run(cb("mcp__engineer-tools__edit_file", {"path": "../../etc/passwd"}, None))
    assert deny["behavior"] == "deny"
    allow = asyncio.run(cb("mcp__engineer-tools__edit_file", {"path": "tests/a.cs"}, None))
    assert allow["behavior"] == "allow"
    bad_bash = asyncio.run(cb("mcp__engineer-tools__bash", {"cmd": "rm -rf /"}, None))
    assert bad_bash["behavior"] == "deny"
    ok_bash = asyncio.run(cb("mcp__engineer-tools__bash", {"cmd": "git status"}, None))
    assert ok_bash["behavior"] == "allow"
