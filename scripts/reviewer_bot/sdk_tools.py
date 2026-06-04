"""In-process ``@tool`` MCP server for the reviewer agent (PoC).

Replaces the OpenAI-format ``TOOLS_SCHEMA`` declared in
``scripts/shared/llm_client.py`` and the dispatch in
``scripts/reviewer_bot/agent.py:_exec_tool`` with the Claude Agent SDK's
``@tool`` decorator + ``create_sdk_mcp_server``. The three tools the
reviewer loop uses -- ``read_paths``, ``grep``, ``finalize_review`` --
are exposed as in-process MCP tools so the loop + transport move to the
SDK while the tool *bodies* stay byte-for-byte (the truncation, byte-cap,
and symlink-containment contracts are load-bearing for downstream parsing
and security).

What is reused verbatim from ``reviewer_bot/agent.py``:
  * ``read_paths_tool`` (driver_root containment + pre-resolve symlink
    reject + post-resolve re-check + 60KB UTF-8 byte cap).
  * ``grep_tool`` (symlink-aware rglob, 50-match cap, 40KB byte cap).
  * ``_unescape_result`` (the PR #317 double-escape salvage, applied to
    PROSE fields only -- body/summary/no_change_reason/gap_analysis).

``finalize_review`` is the loop terminator: ``sdk_agent`` watches for this
tool event and stops the loop immediately (invariant #19 -- no
continuation after finalize; orphan tool_calls discarded). The structured
findings are returned both as MCP ``content`` (so the SDK is satisfied)
and stashed on a module-level holder + the result payload so the caller
can recover the canonical, unescaped review (invariant #20).

The path guard is enforced INSIDE the tool bodies here AND via
``sdk_security.make_can_use_tool`` -- matching the existing
defense-in-depth in ``agent.py``.

Imports of ``claude_agent_sdk`` are guarded so this module imports without
the SDK; ``build_reviewer_server`` raises a clear error if called without
it.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Optional

# NOTE: the audited tool bodies (`read_paths_tool`, `grep_tool`,
# `_unescape_result`) are imported from `reviewer_bot.agent` lazily inside
# `build_reviewer_server` -- NOT at module top. agent.py still pulls in the
# legacy `llm_client.call_llm` transport (until PR6), so deferring the import
# keeps this module importable (e.g. to read ALLOWED_TOOLS / FINALIZE_TOOL_NAME
# in a unit test) without dragging in that surface, matching the guarded-import
# style of the sibling sdk_agent.py / sdk_security.py modules.

try:  # pragma: no cover - exercised only where the SDK is installed
    from claude_agent_sdk import create_sdk_mcp_server, tool

    _SDK_IMPORT_ERROR: Optional[Exception] = None
except Exception as _exc:  # noqa: BLE001
    create_sdk_mcp_server = None  # type: ignore[assignment]
    _SDK_IMPORT_ERROR = _exc

    def tool(name, description, schema):  # type: ignore[no-redef]
        """No-op fallback so the @tool-decorated defs below import cleanly
        when the SDK is absent. The real decorator is used at runtime."""

        def _decorator(fn):
            fn._tool_meta = {"name": name, "description": description, "schema": schema}
            return fn

        return _decorator


SERVER_NAME = "reviewer-tools"

# Fully-qualified tool names for ClaudeAgentOptions(allowed_tools=...).
ALLOWED_TOOLS = [
    f"mcp__{SERVER_NAME}__read_paths",
    f"mcp__{SERVER_NAME}__grep",
    f"mcp__{SERVER_NAME}__finalize_review",
]

FINALIZE_TOOL_NAME = f"mcp__{SERVER_NAME}__finalize_review"


def _text_result(text: str) -> dict[str, Any]:
    """MCP tool return shape: a single text content block."""
    return {"content": [{"type": "text", "text": text}]}


def build_reviewer_server(
    repo_root: Path, driver_root: Optional[Path] = None
) -> "tuple[Any, Any]":
    """Create the in-process MCP server for the reviewer.

    Returns a ``(server, get_last_finalize)`` tuple. ``server`` is the MCP
    server config to pass to ``ClaudeAgentOptions(mcp_servers=...)``;
    ``get_last_finalize()`` is a closure that returns the captured
    ``finalize_review`` arguments (already double-escape-salvaged) once the
    model calls the tool, or ``None`` if it never did. The accessor is returned
    separately because ``create_sdk_mcp_server`` yields an opaque dict-like
    config that doesn't accept attribute assignment.

    ``repo_root`` is the PR's own checkout (this repo). ``driver_root`` is the
    cloned driver repo, present only when the PR touches C# driver tests.

    The exploration tools (``read_paths`` / ``grep``) are rooted at the DRIVER
    clone when reviewing a C# driver-test PR (so the model can judge a test
    against driver behaviour), otherwise at the PR checkout itself — so for
    ANY PR (including the bots' own infra changes) the model can open the
    changed files' surrounding context instead of seeing only the diff.

    No secret-file deny is needed: the reviewer's checkout sets
    ``persist-credentials: false`` (see ``.github/workflows/reviewer-bot.yml``),
    so ``.git/config`` carries no token, and the reviewer posts via the minted
    App token rather than the checkout's git credentials.
    """
    if _SDK_IMPORT_ERROR is not None or create_sdk_mcp_server is None:
        raise RuntimeError(
            "claude_agent_sdk is not installed; cannot build the reviewer "
            f"MCP server. Original import error: {_SDK_IMPORT_ERROR!r}"
        )

    # Lazy import (see module-top note): keep agent.py's transitive surface
    # out of import time. Reuses the audited tool bodies + double-escape
    # salvage VERBATIM.
    from scripts.reviewer_bot.agent import (
        _unescape_result,
        grep_tool,
        read_paths_tool,
    )

    explore_root = driver_root if driver_root is not None else repo_root
    grep_default = "csharp/src/" if driver_root is not None else "."
    src_label = (
        "the cloned driver source (adbc-drivers/databricks)"
        if driver_root is not None
        else "the repository under review (this PR's checkout)"
    )

    # Mutable holder so the captured finalize args survive past the tool
    # call. A dict (not a closure-rebound name) keeps it reachable from the
    # returned server handle below.
    state: dict[str, Any] = {"last_finalize": None}

    @tool(
        "read_paths",
        f"Read one or more files from {src_label}. Use this to see the "
        "surrounding code/implementation needed to judge a finding.",
        {
            "paths": list,
            "reason": str,
        },
    )
    async def read_paths(args: dict[str, Any]) -> dict[str, Any]:
        # Reuses agent.read_paths_tool: containment + symlink reject + byte
        # cap. Run the sync body on a worker thread so a large read can't pin
        # the SDK event loop (matches the engineer _wrap).
        return _text_result(
            await asyncio.to_thread(
                read_paths_tool, args.get("paths") or [],
                driver_root=explore_root,
            )
        )

    @tool(
        "grep",
        f"Search {src_label} for a regex pattern. Returns matching files with "
        f"line numbers. Path defaults to '{grep_default}'.",
        {
            "pattern": str,
            "path": str,
            "reason": str,
        },
    )
    async def grep(args: dict[str, Any]) -> dict[str, Any]:
        # Reuses agent.grep_tool: symlink-aware rglob, 50-match + 40KB caps.
        # On a worker thread so a large-tree grep can't pin the loop.
        return _text_result(
            await asyncio.to_thread(
                grep_tool,
                pattern=args.get("pattern") or "",
                path=args.get("path") or grep_default,
                driver_root=explore_root,
            )
        )

    @tool(
        "finalize_review",
        "Submit the final code review. Call this when you have enough context "
        "to judge the PR. Do NOT emit the review as plain text. The runner "
        "takes your structured arguments as the canonical review.",
        {
            "findings": list,
            "summary": str,
            "suppressions": list,
        },
    )
    async def finalize_review(args: dict[str, Any]) -> dict[str, Any]:
        # Apply the PR #317 double-escape salvage to PROSE fields only
        # (body/summary/no_change_reason/gap_analysis) -- invariant #20.
        # `suggestion`/`file`/`line`/`citation` pass through unchanged.
        state["last_finalize"] = _unescape_result(args)
        # Returning text content satisfies the SDK's tool-result contract; the
        # canonical review is read via the get_last_finalize() closure returned
        # by build_reviewer_server (which closes over `state`).
        return _text_result("review finalized")

    server = create_sdk_mcp_server(
        name=SERVER_NAME,
        version="1.0.0",
        tools=[read_paths, grep, finalize_review],
    )
    # Return the captured-finalize accessor SEPARATELY rather than attaching it
    # to `server`: create_sdk_mcp_server returns an opaque dict-like config that
    # does NOT support attribute assignment (a live PR4 dry-run hit
    # `'dict' object has no attribute 'get_last_finalize'`). The closure over
    # `state` reads the latest finalize args (already double-escape-salvaged)
    # regardless of when the tool fired.
    def get_last_finalize() -> Optional[dict[str, Any]]:
        return state["last_finalize"]

    return server, get_last_finalize


# ─── Followup tool server (read-only, scoped to the PR's test-repo checkout) ──
# The followup decision (retract/clarify/hold/agree) lets the model inspect the
# checked-out test repo via read_paths/grep before choosing an action. Same
# bodies as the review pass (agent.read_paths_tool/grep_tool), just rooted at
# the test repo instead of the cloned driver source. NO finalize tool — the
# action is parsed from the model's final TEXT turn (parse_followup_action).
FOLLOWUP_SERVER_NAME = "reviewer-followup-tools"
FOLLOWUP_ALLOWED_TOOLS = [
    f"mcp__{FOLLOWUP_SERVER_NAME}__read_paths",
    f"mcp__{FOLLOWUP_SERVER_NAME}__grep",
]


def build_followup_server(repo_root: Path) -> Any:
    """In-process MCP server exposing read_paths + grep against ``repo_root``
    (the PR's test-repo checkout). Like the legacy followup tool dispatch, grep
    defaults to "." (the test-repo root), not "csharp/src/"."""
    if _SDK_IMPORT_ERROR is not None or create_sdk_mcp_server is None:
        raise RuntimeError(
            "claude_agent_sdk is not installed; cannot build the followup "
            f"MCP server. Original import error: {_SDK_IMPORT_ERROR!r}"
        )
    from scripts.reviewer_bot.agent import grep_tool, read_paths_tool

    @tool(
        "read_paths",
        "Read one or more files from the checked-out test repo to inspect a "
        "claimed fix before deciding the followup action.",
        {"paths": list, "reason": str},
    )
    async def read_paths(args: dict[str, Any]) -> dict[str, Any]:
        # Run the sync body on a worker thread so a large read can't pin the
        # SDK event loop (matches the review-pass read_paths + engineer _wrap).
        return _text_result(
            await asyncio.to_thread(
                read_paths_tool, args.get("paths") or [], driver_root=repo_root
            )
        )

    @tool(
        "grep",
        "Search the checked-out test repo for a regex pattern (default path: "
        "the repo root).",
        {"pattern": str, "path": str, "reason": str},
    )
    async def grep(args: dict[str, Any]) -> dict[str, Any]:
        # On a worker thread so a large-tree grep can't pin the event loop.
        return _text_result(
            await asyncio.to_thread(
                grep_tool,
                pattern=args.get("pattern") or "",
                path=args.get("path") or ".",
                driver_root=repo_root,
            )
        )

    return create_sdk_mcp_server(
        name=FOLLOWUP_SERVER_NAME, version="1.0.0", tools=[read_paths, grep],
    )
