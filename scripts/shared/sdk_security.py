"""Security gates re-expressed as Claude Agent SDK hooks / callbacks.

Every safety invariant the hand-rolled tool layer enforced
(``scripts/engineer_bot/tools.py``, ``scripts/engineer_bot/env_scrub.py``)
is preserved here -- moved behind SDK hooks (``HookMatcher`` /
``PreToolUse``) and the per-call ``can_use_tool`` permission callback,
rather than dropped.

Mapping (see migration doc section 4):
  * safe_path allowlist/denylist (tools.py:41-62)  -> ``make_safe_path_hook``
    (PreToolUse, Read/Edit/Write/Glob) + ``make_can_use_tool`` (hard deny).
  * bash argv allowlist + per-task timeout (tools.py:363-413)
    -> ``make_bash_allowlist_hook`` (PreToolUse on Bash).
  * env_scrub on subprocess exec (env_scrub.py:12-45)
    -> ``scrubbed_env`` (ported regex table, byte-for-byte) fed to
    ``ClaudeAgentOptions(env=...)``.

The ``env_scrub`` regex table is reused VERBATIM: ``scrubbed_env`` delegates
to ``env_scrub.scrub`` (a thin wrapper, so a future ``env_scrub`` deletion
only needs the table relocated, not the call sites changed).

The ``safe_path`` resolution semantics are re-expressed in ``_resolve_inside``
as an EXACT MIRROR of ``tools.safe_path`` (tools.py:41-62) -- resolve() +
is_relative_to(root) + denied-subpath check -- kept as an independent port
(not an import) so the SDK hooks carry no hard dependency on a ``TaskConfig``
instance. The semantics are identical and covered by unit tests; if
``tools.safe_path`` ever changes, ``_resolve_inside`` must change with it.

Imports of ``claude_agent_sdk`` are guarded so this module is importable
(and unit-testable) without the SDK installed.
"""
from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any, Callable, Optional

# Reuse the audited env-scrub table rather than re-deriving it.
# ``shared.env_scrub.scrub`` carries the deliberate 12-regex table (and its
# intentional avoidance of blanket ^DATABRICKS_/^AWS_ prefixes). The table now
# lives in ``shared/`` (relocated in PR1), so this module has NO dependency on
# ``engineer_bot``. The ``safe_path`` semantics are mirrored independently in
# ``_resolve_inside`` below.
from scripts.shared import env_scrub

try:  # pragma: no cover - exercised only where the SDK is installed
    from claude_agent_sdk import HookMatcher

    _SDK_IMPORT_ERROR: Optional[Exception] = None
except Exception as _exc:  # noqa: BLE001
    HookMatcher = None  # type: ignore[assignment]
    _SDK_IMPORT_ERROR = _exc


# ── Path containment (invariants #1, #2, #9, #35) ─────────────────────
# Tools whose inputs reference filesystem paths and therefore must be
# containment-checked. Maps tool name -> the input keys that hold a path.
_PATH_TOOLS: dict[str, tuple[str, ...]] = {
    # SDK built-in tool names.
    "Read": ("file_path", "path"),
    "Edit": ("file_path", "path"),
    "Write": ("file_path", "path"),
    "Glob": ("path",),
    "Grep": ("path",),
    # Engineer in-process @tool names (matched after stripping the
    # mcp__<server>__ prefix) so the path gate applies to that surface too.
    "read_file": ("file_path", "path"),
    "edit_file": ("file_path", "path"),
    "grep": ("path",),
    "glob": ("path",),
}


def _resolve_inside(
    allowed_root: Path,
    denied_subpaths: tuple[Path, ...],
    rel: str,
) -> Optional[Path]:
    """Mirror of ``tools.safe_path`` (tools.py:41-62), kept independent so
    the SDK hooks have no hard dependency on a ``TaskConfig`` instance.

    Resolve ``rel`` against ``allowed_root``; return ``None`` if it escapes
    the root OR lands inside any ``denied_subpaths`` entry. ``denied_subpaths``
    entries may be files or directories (``is_relative_to`` is True for
    equal paths, so a file entry denies exactly that file).
    """
    root = allowed_root.resolve()
    resolved = (root / rel).resolve()
    if not resolved.is_relative_to(root):
        return None
    for denied in denied_subpaths:
        if resolved.is_relative_to(denied.resolve()):
            return None
    return resolved


def _extract_tool_input(input_data: dict[str, Any]) -> dict[str, Any]:
    """SDK hook payloads wrap the tool args under ``tool_input``; the
    ``can_use_tool`` callback may pass them flat. Accept both shapes.
    """
    if isinstance(input_data.get("tool_input"), dict):
        return input_data["tool_input"]
    return input_data


def _deny(reason: str) -> dict[str, Any]:
    """SDK PreToolUse deny shape (permissionDecision='deny')."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def _allow_with_input(tool_input: dict[str, Any]) -> dict[str, Any]:
    """SDK PreToolUse allow shape that also mutates the tool input
    (used to clamp the Bash timeout)."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
        },
        "tool_input": tool_input,
    }


def make_safe_path_hook(
    allowed_root: Path,
    denied_subpaths: tuple[Path, ...] = (),
) -> Callable[..., Any]:
    """Build a PreToolUse hook that DENIES Read/Edit/Write/Glob/Grep whose
    resolved path escapes ``allowed_root`` or falls inside a denied subpath.

    Re-expresses the ``safe_path`` gate (tools.py:41-62) that previously
    lived inside each tool body. Registered via::

        HookMatcher(matcher="Read|Edit|Write|Glob|Grep", hooks=[hook])

    or per-tool matchers. Returns the SDK deny payload on violation; an
    allow (empty dict) otherwise.
    """

    async def safe_path_hook(input_data, tool_use_id, context):  # noqa: ANN001
        tool_name = input_data.get("tool_name", "")
        keys = _PATH_TOOLS.get(tool_name)
        if not keys:
            return {}
        ti = _extract_tool_input(input_data)
        for key in keys:
            rel = ti.get(key)
            if rel is None:
                continue
            if _resolve_inside(allowed_root, denied_subpaths, str(rel)) is None:
                return _deny(
                    f"{tool_name}: path {rel!r} escapes the sandbox root "
                    f"or is inside a denied subpath"
                )
        return {}

    return safe_path_hook


def make_bash_allowlist_hook(
    allowlist: tuple[tuple[str, ...], ...],
    timeout: int = 300,
) -> Callable[..., Any]:
    """Build a PreToolUse hook for Bash that DENIES any command whose argv
    prefix is not in ``allowlist`` and clamps the per-call timeout.

    Re-expresses the bash gate from ``tools.make_bash_tool`` (tools.py:
    368-389): tokenize with ``shlex`` (shell=False semantics -> metachars
    inert), prefix-tuple match against the allowlist (tools.py:376), and
    set ``tool_input["timeout"]`` to the per-task cap (300s default, 900s for
    csharp_coverage). The ``timeout`` argument here is SECONDS; we write it
    back as a millisecond value (``timeout * 1000``) into the tool input field
    literally named ``timeout`` — the ms unit is UNVERIFIED against the SDK
    Bash tool until PR1's smoke probe (see the body comment).

    Registered via ``HookMatcher(matcher="Bash", hooks=[hook])``.

    NOTE: if the SDK's built-in Bash does not honour an injected timeout,
    the fallback (per the migration doc) is a custom ``@tool bash`` that
    runs ``subprocess.run(timeout=...)`` -- the argv allowlist check below
    is identical either way.
    """

    async def bash_hook(input_data, tool_use_id, context):  # noqa: ANN001
        if input_data.get("tool_name", "") not in ("Bash", "bash"):
            return {}
        ti = _extract_tool_input(input_data)
        cmd = ti.get("command") or ti.get("cmd") or ""
        if not bash_command_allowed(cmd, allowlist):
            try:
                head = shlex.split(cmd)[:3]
            except ValueError:
                head = [cmd[:60]]
            return _deny(f"command not in allowlist: {head!r}")
        # Enforce the per-task cap. NOTE: the SDK Bash tool's timeout field
        # NAME and UNIT (ms vs s) are UNVERIFIED until PR1's smoke exercises a
        # real Bash call. Until then we SET the cap unconditionally (a ceiling)
        # rather than clamping a model-supplied value: this avoids both the
        # falsy-0 trap (`if existing` would replace a 0 with the full cap) and
        # a unit-mismatch where min(60, 900_000) would yield a 60ms timeout.
        timeout_ms = timeout * 1000
        ti = dict(ti)
        ti["timeout"] = timeout_ms
        return _allow_with_input(ti)

    return bash_hook


def bash_command_allowed(
    cmd: str,
    allowlist: tuple[tuple[str, ...], ...],
) -> bool:
    """Return True iff ``cmd`` tokenizes to a non-empty argv whose prefix
    matches an allowlist entry.

    Exact port of the gate in ``tools.make_bash_tool`` (tools.py:370-377):
    ``shlex.split`` (raises -> reject), empty argv -> reject, then
    ``any(tuple(argv[:len(p)]) == p for p in allowlist)``.
    """
    try:
        argv = shlex.split(cmd)
    except ValueError:
        return False
    if not argv:
        return False
    return any(tuple(argv[: len(p)]) == p for p in allowlist)


def make_can_use_tool(
    allowed_root: Path,
    *,
    denied_subpaths: tuple[Path, ...] = (),
    bash_allowlist: tuple[tuple[str, ...], ...] = (),
    allowed_tool_names: tuple[str, ...] = (),
) -> Callable[..., Any]:
    """Build the per-call ``can_use_tool`` permission callback: a HARD
    pre-execution deny that mirrors the path + bash gates.

    This is defense-in-depth alongside the PreToolUse hooks (and, for the
    in-process ``@tool`` bodies, alongside the checks inside the tool
    itself -- matching the existing double-layered containment in
    ``reviewer_bot/agent.py``). It returns the SDK ``can_use_tool`` allow
    /deny decision shape.
    """

    async def can_use_tool(tool_name, input_data, context):  # noqa: ANN001
        ti = _extract_tool_input(input_data)
        # In-process @tool names arrive fully-qualified (mcp__<server>__<tool>);
        # strip to the bare name so the path/bash gates ALSO apply to that
        # surface (second layer alongside the tool bodies). Without this the
        # denied_subpaths/bash_allowlist args were inert for mcp__* calls. The
        # allowed_tool_names allowlist below still matches the FQ name. (Review)
        base = tool_name.rsplit("__", 1)[-1]
        keys = _PATH_TOOLS.get(base)
        if keys:
            for key in keys:
                rel = ti.get(key)
                if rel is not None and _resolve_inside(
                    allowed_root, denied_subpaths, str(rel)
                ) is None:
                    return {
                        "behavior": "deny",
                        "message": f"{tool_name}: path {rel!r} outside sandbox",
                    }
            return {"behavior": "allow", "updatedInput": ti}
        if base in ("Bash", "bash"):
            cmd = ti.get("command") or ti.get("cmd") or ""
            if not bash_command_allowed(cmd, bash_allowlist):
                return {"behavior": "deny", "message": "command not in allowlist"}
            return {"behavior": "allow", "updatedInput": ti}
        # Non-path / non-Bash tools (e.g. MCP @tool names
        # mcp__<server>__<tool>): if an explicit allowed_tool_names tuple was
        # provided, deny anything not in it. When it is EMPTY (the default),
        # this callback does not re-deny — default-deny is then enforced at
        # the SDK layer by ClaudeAgentOptions(allowed_tools=[...]) (invariant
        # #39); this callback only adds the path + bash hard-denies above.
        # Pass allowed_tool_names to make the callback itself default-deny.
        if allowed_tool_names and tool_name not in allowed_tool_names:
            return {"behavior": "deny", "message": f"tool {tool_name!r} not permitted"}
        return {"behavior": "allow", "updatedInput": ti}

    return can_use_tool


def make_pre_tool_hooks(
    allowed_root: Path,
    *,
    denied_subpaths: tuple[Path, ...] = (),
    bash_allowlist: tuple[tuple[str, ...], ...] = (),
    bash_timeout: int = 300,
) -> Any:
    """Assemble the ``hooks`` mapping for ``ClaudeAgentOptions``: the
    safe_path PreToolUse hook (Read/Edit/Write/Glob/Grep) plus the Bash
    allowlist+timeout PreToolUse hook.

    Returns ``{"PreToolUse": [HookMatcher(...), ...]}``. Raises a clear
    error if the SDK is not installed (the matchers are SDK types).
    """
    if _SDK_IMPORT_ERROR is not None or HookMatcher is None:
        raise RuntimeError(
            "claude_agent_sdk is not installed; cannot build HookMatchers. "
            f"Original import error: {_SDK_IMPORT_ERROR!r}"
        )
    return {
        "PreToolUse": [
            HookMatcher(
                matcher="Read|Edit|Write|Glob|Grep",
                hooks=[make_safe_path_hook(allowed_root, denied_subpaths)],
            ),
            HookMatcher(
                matcher="Bash",
                hooks=[make_bash_allowlist_hook(bash_allowlist, bash_timeout)],
            ),
        ]
    }


def scrubbed_env(base: Optional[dict[str, str]] = None) -> dict[str, str]:
    """Return the process environment with credential-shaped vars removed,
    for ``ClaudeAgentOptions(env=...)``.

    Delegates to ``env_scrub.scrub`` so the 12-regex table (and its
    deliberate avoidance of blanket ^DATABRICKS_/^AWS_/^AZURE_ prefixes)
    is reused VERBATIM -- the doc forbids rewriting it. Because the SDK's
    Bash tool inherits the CLI process env, scrubbing the options' ``env``
    covers spawned subprocesses (invariant #5).

    NOTE (migration doc risk): verify in the PR1 smoke test that the SDK
    does not re-inject ``ANTHROPIC_AUTH_TOKEN`` / ``DATABRICKS_TOKEN`` into
    the Bash subprocess env; if it does, add a PostToolUse stdout/stderr
    redaction hook as defense-in-depth.
    """
    import os

    source = base if base is not None else dict(os.environ)
    return env_scrub.scrub(source)


# Re-export the raw scrub under this module's namespace. The canonical table
# now lives in ``shared/env_scrub.py`` (relocated in PR1), so PR6's deletion of
# ``engineer_bot/env_scrub.py`` is a pure deletion of a re-export shim -- it no
# longer threatens this import.
scrub_env = env_scrub.scrub
