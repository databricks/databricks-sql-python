import json
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.reviewer_bot import sdk_tools
from scripts.reviewer_bot.agent import (
    run_agent, read_paths_tool, grep_tool, _exec_tool, AgentLoopError,
    _unescape_jsonish, _unescape_result, _looks_double_escaped,
)


# ── read_paths_tool tests ─────────────────────────────────────────

def test_read_paths_returns_contents(fake_driver_tree):
    out = read_paths_tool(
        ["csharp/src/Authentication/AuthHandler.cs"],
        driver_root=fake_driver_tree,
    )
    assert "=== csharp/src/Authentication/AuthHandler.cs ===" in out
    assert "auth handler" in out


def test_read_paths_rejects_escape(fake_driver_tree):
    out = read_paths_tool(
        ["../../etc/passwd"],
        driver_root=fake_driver_tree,
    )
    assert "REJECTED" in out
    assert "escapes" in out


def test_read_paths_not_found(fake_driver_tree):
    out = read_paths_tool(
        ["csharp/src/DoesNotExist.cs"],
        driver_root=fake_driver_tree,
    )
    assert "NOT FOUND" in out


def test_read_paths_respects_byte_cap(fake_driver_tree):
    out = read_paths_tool(
        ["csharp/src/Authentication/AuthHandler.cs",
         "csharp/src/Authentication/TokenManager.cs",
         "csharp/src/Statements/StatementExecutor.cs"],
        driver_root=fake_driver_tree,
        byte_cap=100,
    )
    assert "TRUNCATED" in out


def test_read_paths_empty_list(fake_driver_tree):
    out = read_paths_tool([], driver_root=fake_driver_tree)
    assert "no paths" in out


# ── grep_tool tests ───────────────────────────────────────────────

def test_grep_finds_matches(fake_driver_tree):
    out = grep_tool(
        pattern="auth handler",
        path="csharp/src/",
        driver_root=fake_driver_tree,
    )
    assert "AuthHandler.cs" in out
    assert "auth handler" in out


def test_grep_no_match(fake_driver_tree):
    out = grep_tool(
        pattern="nonexistent_pattern_xyz",
        path="csharp/src/",
        driver_root=fake_driver_tree,
    )
    assert "no matches" in out


def test_grep_rejects_escape(fake_driver_tree):
    out = grep_tool(
        pattern="foo",
        path="../etc",
        driver_root=fake_driver_tree,
    )
    assert "REJECTED" in out


def test_grep_invalid_regex(fake_driver_tree):
    out = grep_tool(
        pattern="[unclosed",
        path="csharp/src/",
        driver_root=fake_driver_tree,
    )
    assert "invalid regex" in out


def test_grep_single_file_path(fake_driver_tree):
    """The followup schema advertises `path` as accepting either a
    directory or a file. `rglob("*")` on a file yields nothing, so
    without single-file handling a grep against a cited file (e.g.
    `.github/workflows/foo.yml`) would silently return "no matches"
    and fail to verify the fix. Pin the file-path branch.
    """
    out = grep_tool(
        pattern="auth handler",
        path="csharp/src/Authentication/AuthHandler.cs",
        driver_root=fake_driver_tree,
    )
    assert "AuthHandler.cs" in out
    assert "auth handler" in out


def test_grep_rejects_single_file_symlink(fake_driver_tree, tmp_path_factory):
    """When the model targets a single-file path that is itself a
    symlink, reject it the same way `read_paths_tool` does. The
    pre-resolve `is_symlink()` check catches this even though
    `target_root.resolve()` would canonicalize the path away.
    """
    outside = tmp_path_factory.mktemp("outside") / "secret.txt"
    outside.write_text("UNIQUE_SECRET_BODY_CONTENT\n")
    sneak = fake_driver_tree / "csharp" / "src" / "sneak.cs"
    sneak.symlink_to(outside)
    out = grep_tool(
        pattern="UNIQUE_SECRET_BODY",
        path="csharp/src/sneak.cs",
        driver_root=fake_driver_tree,
    )
    assert "UNIQUE_SECRET_BODY_CONTENT" not in out
    assert "REJECTED" in out
    # The single-file symlink must be rejected; accept either guard that
    # enforces containment — the pre-resolve symlink check OR the post-resolve
    # "escapes driver root" check (which fires when the symlink target's temp
    # dir resolves outside driver_root, e.g. under /tmp or /private on macOS).
    # The security-critical assertions are the two above (no leak + REJECTED).
    assert "symlink" in out.lower() or "escapes" in out.lower()


# ── Symlink rejection (PR #414 review thread r3326846644) ─────────
#
# When `grep_tool` / `read_paths_tool` are reused from the followup
# code path, the `driver_root` is the PR's checked-out source —
# attacker-controlled content. A symlink under the root pointing at
# `/etc/passwd` etc. would otherwise be read and its contents
# inlined into the tool result. These tests pin the symlink-rejection
# behavior so a regression can't silently re-open that hole.
#
# Following the existing convention in
# `scripts/engineer_bot/tests/test_tools.py:TestSymlinkRejection` —
# CI is Linux-only, so we just call `.symlink_to()` directly without
# a platform skip. Same approach as the sibling test class.


def test_grep_skips_symlink_to_outside_root(fake_driver_tree, tmp_path_factory):
    """A symlink under the driver tree pointing to a file OUTSIDE the
    root must not have its content read by grep_tool, even when the
    pattern would otherwise match the target file's content.
    """
    outside = tmp_path_factory.mktemp("outside") / "secret.txt"
    # Use a pattern that DIFFERS from the file contents so the
    # "no matches for /PATTERN/ in ..." echo doesn't trip the assert.
    outside.write_text("UNIQUE_SECRET_BODY_CONTENT\n")
    # Symlink at csharp/src/sneak -> outside/secret.txt
    sneak = fake_driver_tree / "csharp" / "src" / "sneak"
    sneak.symlink_to(outside)
    out = grep_tool(
        pattern="UNIQUE_SECRET_BODY",
        path="csharp/src/",
        driver_root=fake_driver_tree,
    )
    # The symlink target's contents must NOT appear in results.
    # Use the CONTENT marker (which differs from the search pattern)
    # so the echoed "no matches for /PATTERN/" string can't false-pass.
    assert "UNIQUE_SECRET_BODY_CONTENT" not in out
    # The symlink path itself must NOT be reported as a match.
    assert "sneak" not in out


def test_grep_skips_symlink_inside_root(fake_driver_tree):
    """A symlink whose resolved target stays INSIDE the root is
    still skipped (defense layer 1 — `is_symlink()` check). The
    underlying file is found via its real path; the symlink is just
    a duplicate alias we don't need to traverse.
    """
    real = fake_driver_tree / "csharp" / "src" / "Authentication" / "AuthHandler.cs"
    # Symlink that lives in csharp/src/ pointing at the same file
    # through a different path. Iff the symlink were followed, the
    # content "auth handler" would appear twice in the results.
    alias = fake_driver_tree / "csharp" / "src" / "auth_alias.cs"
    alias.symlink_to(real)
    out = grep_tool(
        pattern="auth handler",
        path="csharp/src/",
        driver_root=fake_driver_tree,
    )
    # Found via the real path:
    assert "AuthHandler.cs" in out
    # NOT also reported via the symlink alias:
    assert "auth_alias.cs" not in out


def test_read_paths_rejects_explicit_symlink(fake_driver_tree, tmp_path_factory):
    """An explicit `read_paths_tool` call with a path that IS a
    symlink should be rejected as such — defense-in-depth on top of
    the existing resolve-and-contain check.
    """
    outside = tmp_path_factory.mktemp("outside") / "secret.txt"
    outside.write_text("UNIQUE_SECRET_MARKER\n")
    sneak = fake_driver_tree / "csharp" / "src" / "sneak.cs"
    sneak.symlink_to(outside)
    out = read_paths_tool(
        ["csharp/src/sneak.cs"],
        driver_root=fake_driver_tree,
    )
    assert "UNIQUE_SECRET_MARKER" not in out
    # The rejection reason should clearly say it's a symlink, not
    # the generic "escapes driver root" — symlink targets that point
    # OUTSIDE the root would have been caught by the existing
    # is_relative_to check; this layer also catches inside-root
    # symlinks the model might use as obfuscation.
    assert "symlink" in out.lower() or "escapes" in out.lower()


def test_read_paths_rejects_symlink_inside_root(fake_driver_tree):
    """Symlink whose resolved target stays INSIDE the root must still
    be rejected — this is the case the new `is_symlink()` defense
    layer was added to cover. The pre-existing `target.resolve()` +
    `is_relative_to(driver_root_resolved)` check would HAPPILY admit
    this (resolved path is inside the root), so without the explicit
    symlink check the alias contents would be returned. The grep
    variant `test_grep_skips_symlink_inside_root` covers the analogous
    case for grep_tool; this is the read_paths_tool counterpart.
    See PR #414 review thread r3326846644.
    """
    real = fake_driver_tree / "csharp" / "src" / "Authentication" / "AuthHandler.cs"
    assert real.is_file(), "fake_driver_tree fixture changed shape"
    alias = fake_driver_tree / "csharp" / "src" / "auth_alias.cs"
    alias.symlink_to(real)
    out = read_paths_tool(
        ["csharp/src/auth_alias.cs"],
        driver_root=fake_driver_tree,
    )
    # The aliased file's content must NOT be inlined under the
    # symlink path — the rejection must fire FIRST.
    assert "auth handler" not in out
    # Rejection reason must be the symlink-specific one (not the
    # generic "escapes driver root" — which wouldn't apply here
    # since the resolved target IS inside the root).
    assert "symlink" in out.lower()


# ── _exec_tool dispatcher tests ───────────────────────────────────

def test_exec_tool_no_driver_returns_error(fake_driver_tree):
    out = _exec_tool("read_paths", {"paths": ["x"]}, driver_root=None)
    assert "ERROR" in out
    assert "no driver source" in out.lower()


def test_exec_tool_unknown_returns_error(fake_driver_tree):
    out = _exec_tool("nonexistent_tool", {}, driver_root=fake_driver_tree)
    assert "unknown tool" in out


# ── run_agent SDK-contract tests (mocked SDK boundary) ────────────
# The agentic loop now lives in the SDK. run_agent builds the reviewer @tool
# server, drives sdk_agent.run_agent, and returns the captured finalize_review
# findings (or falls back to JSON extraction). We mock the SDK boundary
# (build_reviewer_server + sdk_agent.run_agent) since the SDK isn't installed
# in unit-test envs.

def _fake_server(finalize):
    """Stand-in for build_reviewer_server's (server, get_last_finalize) tuple:
    an opaque server object + a closure returning the captured finalize args."""
    return object(), (lambda: finalize)


def _fake_result(final_text=""):
    from scripts.shared.sdk_agent import AgentResult
    return AgentResult(final_text=final_text, turns=1, stop_reason="end_turn")


def test_run_agent_returns_finalize_capture():
    """finalize_review's @tool stashes the (already-unescaped) findings on the
    server; run_agent returns them verbatim."""
    findings = {
        "findings": [{"id": "F1", "severity": "low", "body": "ok"}],
        "summary": "done",
    }
    with patch("scripts.reviewer_bot.agent.sdk_tools.build_reviewer_server",
               return_value=_fake_server(findings)), \
         patch("scripts.reviewer_bot.agent.sdk_agent.run_agent",
               return_value=_fake_result()):
        result = run_agent(endpoint="x", token="y", system="s", user="u",
                           driver_root=None, max_turns=3)
    assert result["findings"][0]["id"] == "F1"
    assert result["summary"] == "done"


def test_run_agent_passes_reviewer_tools_and_server():
    """Wiring guard: sdk_agent.run_agent is driven with the reviewer tool
    allowlist + the built MCP server."""
    fake = _fake_server({"findings": [], "summary": "ok"})  # (server_obj, getter)
    with patch("scripts.reviewer_bot.agent.sdk_tools.build_reviewer_server",
               return_value=fake), \
         patch("scripts.reviewer_bot.agent.sdk_agent.run_agent",
               return_value=_fake_result()) as mock_run:
        run_agent(endpoint="x", token="y", system="s", user="u",
                  driver_root=None, max_turns=7)
    kw = mock_run.call_args.kwargs
    assert kw["allowed_tools"] == list(sdk_tools.ALLOWED_TOOLS)
    # run_agent unpacks (server, getter); only the server obj goes to the SDK.
    assert kw["mcp_servers"] == {sdk_tools.SERVER_NAME: fake[0]}
    assert kw["max_turns"] == 7


def test_run_agent_falls_back_to_text_extraction():
    """No finalize_review call → salvage brace-balanced JSON from final_text."""
    text = 'Here is the review: {"findings": [], "summary": "fallback"}'
    with patch("scripts.reviewer_bot.agent.sdk_tools.build_reviewer_server",
               return_value=_fake_server(None)), \
         patch("scripts.reviewer_bot.agent.sdk_agent.run_agent",
               return_value=_fake_result(final_text=text)):
        result = run_agent(endpoint="x", token="y", system="s", user="u",
                           driver_root=None, max_turns=3)
    assert result == {"findings": [], "summary": "fallback"}


def test_run_agent_raises_when_no_finalize_and_unparseable():
    with patch("scripts.reviewer_bot.agent.sdk_tools.build_reviewer_server",
               return_value=_fake_server(None)), \
         patch("scripts.reviewer_bot.agent.sdk_agent.run_agent",
               return_value=_fake_result(final_text="just prose, no json here")):
        with pytest.raises(AgentLoopError):
            run_agent(endpoint="x", token="y", system="s", user="u",
                      driver_root=None, max_turns=3)


# ── _unescape_jsonish / _unescape_result tests ────────────────────


def test_looks_double_escaped_detects_pathology():
    """The actual PR #317 failure mode: multi-paragraph prose with no real
    newlines but with multiple literal `\\n` runs."""
    assert _looks_double_escaped("Overview\\n\\nDetails follow\\n\\nMore.")
    # Quote-doubling signal works too:
    assert _looks_double_escaped("I said \\\"hello\\\" and \\\"goodbye\\\"")


def test_looks_double_escaped_spares_real_prose():
    """Any string with even one real newline is treated as legitimate
    prose, even if it mentions `\\n` literally (a docstring discussing
    escape sequences). The cost of corrupting docs > occasional unfixed
    pathological output."""
    assert not _looks_double_escaped("Use `\\n` to start a new line.\nDetails follow.")
    assert not _looks_double_escaped("Multi\nline\nprose")
    # Single literal mention without real newlines is also not strong enough:
    assert not _looks_double_escaped("This converts \\n to newlines")
    # Empty / short strings:
    assert not _looks_double_escaped("")
    assert not _looks_double_escaped("ok")


def test_unescape_jsonish_unescapes_pathological_strings():
    """When _looks_double_escaped returns True, the escape sequences are
    converted to their intended characters."""
    s = 'Overview\\n\\nThis PR is titled \\"foo\\".\\n\\nDetails below.'
    assert _unescape_jsonish(s) == 'Overview\n\nThis PR is titled "foo".\n\nDetails below.'


def test_unescape_jsonish_preserves_prose_discussing_escape_sequences():
    """Prose that legitimately discusses `\\n` (e.g. a tutorial or comment
    explaining escape sequences) must NOT be silently corrupted. As long
    as the string has at least one real newline, treat it as prose."""
    prose = "Use `\\n` at the end of each line to break.\nFor tabs use `\\t`."
    assert _unescape_jsonish(prose) == prose


def test_unescape_jsonish_preserves_single_literal_mention():
    """A single literal `\\n` in a string with no real newlines is too weak
    a signal — could be legitimate prose. Don't unescape."""
    assert _unescape_jsonish("This converts \\n to newlines") == "This converts \\n to newlines"


def test_unescape_jsonish_non_string_passthrough():
    """Anything not a string passes through unchanged."""
    assert _unescape_jsonish(None) is None
    assert _unescape_jsonish(42) == 42
    assert _unescape_jsonish(True) is True


def test_unescape_result_walks_prose_fields_only():
    """_unescape_result unescapes strings under prose keys (body, summary)
    but leaves code-carrying fields (suggestion, file, citation, id) alone.

    Reason: `suggestion` is inserted verbatim into GitHub's ```suggestion```
    block. If a model legitimately emits `\\n` in suggested code (e.g. a
    regex pattern, or a Python escape-sequence example), unescaping it
    would silently corrupt the suggestion."""
    obj = {
        # Pathological — no real newlines, multiple `\\n` literals → unescape
        "summary": "Overview\\n\\nThis PR...\\n\\nMore.",
        "findings": [
            {
                "id": "F1",
                # Pathological prose → unescape
                "body": "First finding.\\n\\nDetails here.\\n\\nFootnote.",
                "suggestion": "regex = re.compile(r'\\n+')",  # MUST stay literal
                "citation": "CLAUDE.md:55",
                "file": "tests/foo.py",
            },
            {
                "id": "F2",
                # Single-paragraph prose, no escapes → unchanged
                "body": "Second finding",
                "suggestion": "print('multi\\nline')",  # MUST stay literal
            },
            {
                "id": "F3",
                # Prose discussing escape sequences (has real newline + literals)
                # → heuristic should NOT trigger; preserve verbatim
                "body": "Use `\\n` for a newline.\nFor tabs use `\\t`.",
            },
        ],
        "count": 3,
    }
    result = _unescape_result(obj)
    # Pathological prose fields: unescaped
    assert result["summary"] == "Overview\n\nThis PR...\n\nMore."
    assert result["findings"][0]["body"] == "First finding.\n\nDetails here.\n\nFootnote."
    # Single-paragraph prose with no escapes: unchanged
    assert result["findings"][1]["body"] == "Second finding"
    # Prose discussing escape sequences: preserved verbatim (real newline present)
    assert result["findings"][2]["body"] == "Use `\\n` for a newline.\nFor tabs use `\\t`."
    # Code-carrying fields: preserved verbatim regardless of escape pattern
    assert result["findings"][0]["suggestion"] == "regex = re.compile(r'\\n+')"
    assert result["findings"][1]["suggestion"] == "print('multi\\nline')"
    # Identifier-shaped fields: preserved
    assert result["findings"][0]["citation"] == "CLAUDE.md:55"
    assert result["findings"][0]["file"] == "tests/foo.py"
    assert result["findings"][0]["id"] == "F1"
    # Non-string scalars: preserved
    assert result["count"] == 3


# NOTE: the old loop-specific run_agent tests (finalize-wins-over-bundled-tools,
# finalize-only-no-warning, double-escape-of-tool-call-args) were removed in the
# SDK migration (PR4): that behavior is now either internal to the SDK loop or
# enforced by the finalize_review @tool in sdk_tools (which applies
# _unescape_result). The double-escape unescaping itself is covered by the
# _unescape_jsonish / _unescape_result tests above.
