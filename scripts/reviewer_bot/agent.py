"""Agentic LLM loop with tool use.

Wraps llm_client.call_llm in a multi-turn loop. The LLM can either
emit a final response (JSON block) or call one of the available
tools; the runner executes the tool and feeds the result back into
the conversation.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

from scripts.reviewer_bot import sdk_tools
from scripts.shared import llm_client, sdk_agent, sdk_security

if TYPE_CHECKING:
    from .observer import Observer


class AgentLoopError(RuntimeError):
    """The agent loop did not produce a final response."""


_JSONISH_ESCAPES = (
    ('\\n', '\n'),
    ('\\r', '\r'),
    ('\\t', '\t'),
    ('\\"', '"'),
)


_PROSE_FIELDS = frozenset({"body", "summary", "no_change_reason", "gap_analysis"})


def _looks_double_escaped(s: str) -> bool:
    """Heuristic for detecting strings that survived json.loads with their
    escapes intact (the observed double-escape pathology from PR #317).

    A string is treated as pathologically double-escaped ONLY when BOTH:
      1. It contains NO real newlines (real multi-paragraph prose always has
         at least one — its absence is a strong double-escape signal).
      2. It contains 2+ literal `\\n` runs OR 2+ literal `\\"` runs (the
         model consistently double-escaped paragraph breaks or quoted
         phrases throughout the string).

    Prose strings that mention escape sequences in passing (e.g. "use `\\n`
    to start a new line.") have at least one real newline and are spared.
    This is the only false-negative we accept; the alternative (over-
    aggressive replacement) corrupts documentation that talks about escapes.
    """
    if '\n' in s:
        return False
    return s.count('\\n') >= 2 or s.count('\\"') >= 2


def _unescape_jsonish(s: str) -> str:
    """Convert literal backslash-escape sequences that survived json.loads,
    but ONLY when the string looks pathologically double-escaped.

    Some models double-escape strings in tool-call arguments — the JSON parser
    decodes one layer but the literal two-character sequence `\\n` remains
    instead of an actual newline. We detect that case via `_looks_double_escaped`
    and convert the common escape sequences.

    Even with the heuristic, only apply to PROSE fields (body, summary,
    no_change_reason, gap_analysis). The `suggestion` field is inserted
    verbatim into GitHub's ```suggestion``` block, so its content must be
    preserved exactly. See `_unescape_result` for the field-scoped dispatch.
    """
    if not isinstance(s, str):
        return s
    if not _looks_double_escaped(s):
        return s
    for needle, repl in _JSONISH_ESCAPES:
        s = s.replace(needle, repl)
    return s


# NOTE: _unescape_result, read_paths_tool, and grep_tool are reused VERBATIM
# by scripts/reviewer_bot/sdk_tools.py (the SDK @tool server). When PR4
# collapses this module onto the SDK, keep these three (or move them to a
# stable home) -- do not delete them as part of the loop/dispatch removal.
def _unescape_result(obj: Any, *, field_name: Optional[str] = None) -> Any:
    """Recursively unescape strings in prose fields only.

    `field_name` is the dict key under which `obj` lives in its parent.
    Strings under a prose key (body, summary, …) are unescaped; strings
    under code-carrying keys (suggestion, file, line, citation, …) are
    passed through unchanged.

    For lists, the element shares the parent's `field_name` (so a list
    of prose strings under `body` is still treated as prose).
    """
    if isinstance(obj, dict):
        return {k: _unescape_result(v, field_name=k) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_unescape_result(v, field_name=field_name) for v in obj]
    if isinstance(obj, str) and field_name in _PROSE_FIELDS:
        return _unescape_jsonish(obj)
    return obj


def run_agent(
    *,
    endpoint: str,
    token: str,
    system: str,
    user: str,
    driver_root: Optional[Path] = None,
    content_root: Optional[Path] = None,
    max_turns: int = 20,
    observer: Optional["Observer"] = None,
) -> dict[str, Any]:
    """Run the agentic loop until the model calls finalize_review.

    `driver_root` is the cloned driver repo (present for C# driver-test PRs).
    The read_paths / grep tools root there when it's set, else at
    `content_root` — the PR-head checkout the caller already resolved (see
    run_review._content_root) — so for any PR the model can open the changed
    files' surrounding context rather than reviewing only the diff.

    `content_root` is passed in by run_review so the prefetched repo-rules /
    specs and the agent's explore tools share ONE source of truth. When the
    caller omits it we fall back to the same env-then-cwd derivation
    (REVIEW_CONTENT_ROOT else Path.cwd()), preserving standalone behavior.

    The model is expected to call `finalize_review` with the structured
    review as its arguments. If the model instead emits text (finish=stop),
    we fall back to brace-balanced JSON extraction from the text as a
    safety net.
    """
    # Reviewer toolset (read_paths / grep / finalize_review) as an in-process
    # @tool MCP server. The tool BODIES are reused verbatim from this module
    # (read_paths_tool / grep_tool driver_root containment + byte caps; the
    # finalize_review @tool applies the PR #317 double-escape salvage). The SDK
    # replaces the hand-rolled call_llm loop + tool dispatch + finalize parsing.
    # read_paths/grep root at the cloned driver source for a C# driver-test PR,
    # else at the PR's own checkout (cwd) so the model can open the changed
    # files' context for ANY PR — not just review the bare diff. The reviewer's
    # checkout uses persist-credentials:false (see reviewer-bot.yml) so
    # .git/config carries no token, but that's the primary control, not the
    # only one: read_paths_tool/grep_tool both deny `.git` paths at the code
    # level (read_paths' `.git` parts check + grep's _GREP_EXCLUDE_DIRS), so
    # safety isn't coupled to a single YAML line that could regress.
    # Explore root for read_paths/grep on a non-driver PR. Prefer
    # REVIEW_CONTENT_ROOT (the separate PR-head checkout the workflow exports on
    # workflow_dispatch) over cwd: on that fork-guard-exempt trigger the bot CODE
    # runs from the trusted default-branch checkout (== cwd), so cwd is NOT the
    # PR tree. Reading the PR's content is safe (read_paths/grep enforce
    # path-escape/.git/symlink guards); executing its code is what we refuse to
    # do. When unset (pull_request), cwd IS the PR (merge-ref) tree.
    #
    # Prefer the caller-supplied content_root (run_review._content_root) so the
    # prefetched repo-rules/specs and these explore tools read from the SAME
    # tree — a single source of truth instead of two independent env reads that
    # could diverge (REPO_ROOT vs cwd) if the module is ever invoked from a
    # different cwd. The env-then-cwd derivation remains the standalone default.
    repo_root = content_root or Path(os.environ.get("REVIEW_CONTENT_ROOT") or Path.cwd())
    server, get_last_finalize = sdk_tools.build_reviewer_server(repo_root, driver_root)
    # Surface-level default-deny: only the three reviewer tools are permitted.
    # (Containment for read_paths/grep lives in the @tool bodies; the reviewer
    # uses no built-in path tools, so the path branch of can_use_tool is inert.)
    can_use_tool = sdk_security.make_can_use_tool(
        driver_root or repo_root,
        allowed_tool_names=tuple(sdk_tools.ALLOWED_TOOLS),
    )

    result = sdk_agent.run_agent(
        endpoint=endpoint, token=token, model="databricks-claude-opus-4-8",
        system=system, prompt=user,
        cwd=str(driver_root or repo_root),
        allowed_tools=list(sdk_tools.ALLOWED_TOOLS),
        mcp_servers={sdk_tools.SERVER_NAME: server},
        can_use_tool=can_use_tool,
        env=sdk_security.scrubbed_env(),
        max_turns=max_turns,
    )

    # Best-effort telemetry: the per-turn loop is owned by the SDK now, so we
    # record ONE rollup turn from the AgentResult — but with the REAL signal
    # (every tool name the model called across the run + the terminal token
    # usage), not the old 0/0/(none) placeholders that made every run look idle.
    if observer is not None and hasattr(observer, "record_turn"):
        try:
            observer.record_turn(
                turn=result.turns, finish_reason=result.stop_reason,
                tool_calls=result.tool_calls,
                prompt_tokens=result.prompt_tokens,
                completion_tokens=result.completion_tokens,
            )
            if result.total_cost_usd:
                observer.log(f"Phase 3 cost: ${result.total_cost_usd:.4f}")
        except Exception:  # noqa: BLE001 - telemetry must never fail the run
            pass

    # finalize_review terminates the review: its @tool stashes the structured,
    # already-unescaped args on the server (invariants #19/#20). The model emits
    # a trailing end_turn message after the tool result; we read the captured
    # findings regardless of that final turn.
    findings = get_last_finalize()
    if findings is not None:
        return findings

    # Fallback: the model emitted the review as text instead of calling
    # finalize_review. Salvage via brace-balanced JSON extraction (PR #317).
    try:
        return _unescape_result(llm_client.extract_json_block(result.final_text))
    except llm_client.ParseError as e:
        raise AgentLoopError(
            "model did not call finalize_review and the final text was not "
            f"parseable JSON: {e}"
        ) from e


def _exec_tool(
    name: str,
    args: dict[str, Any],
    driver_root: Optional[Path],
) -> str:
    """Execute an exploration tool (read_paths / grep) and return the
    string content for the resulting tool message. The `finalize_review`
    tool is NOT dispatched here — the caller (run_agent) inspects for
    it and terminates the loop on detection.
    """
    if driver_root is None:
        return (
            "[ERROR: no driver source cloned for this PR. Finalize the review "
            "without driver-source context; do not retry the tool.]"
        )
    if name == "read_paths":
        return read_paths_tool(args.get("paths") or [], driver_root=driver_root)
    if name == "grep":
        return grep_tool(
            pattern=args.get("pattern") or "",
            path=args.get("path") or "csharp/src/",
            driver_root=driver_root,
        )
    return f"[ERROR: unknown tool '{name}']"


# These two helpers (`read_paths_tool` and `grep_tool`) are intentionally
# public: they're reused verbatim by the SDK @tool servers in
# `reviewer_bot/sdk_tools.py` (both `build_reviewer_server`, against the
# cloned driver source, and `build_followup_server`, against the test-repo
# checkout) so the containment + byte-cap contracts stay byte-for-byte.
# Keep names stable; the `_tool` suffix mirrors the schema name without
# colliding with any stdlib helpers (`grep` is generic).
def read_paths_tool(
    paths: list[str],
    *,
    driver_root: Path,
    byte_cap: int = 60_000,
) -> str:
    """Read multiple files. Refuse path escapes. Cap total bytes.

    Byte budget is measured in UTF-8 bytes of the FULL emitted section
    (header + body + the "\\n\\n" separator that join() will insert
    between sections). `len(text)` on a str is a character count, which
    under-reports any non-ASCII content; the parameter is named
    `byte_cap` and the truncation marker says "byte", so the unit must
    match. Mirrors the accounting in `_read_driver_files`.
    """
    driver_root_resolved = driver_root.resolve()
    out: list[str] = []
    SEP_BYTES = len("\n\n".encode("utf-8"))
    used = 0
    for p in paths:
        try:
            target = (driver_root / p).resolve()
        except (OSError, ValueError) as e:
            section = f"=== {p} ===\n[REJECTED: invalid path: {e}]"
            section_bytes = len(section.encode("utf-8"))
            added = section_bytes + (SEP_BYTES if out else 0)
            if used + added > byte_cap:
                out.append(f"=== {p} ===\n[TRUNCATED: total {byte_cap}-byte cap reached]")
                break
            out.append(section)
            used += added
            continue
        if not target.is_relative_to(driver_root_resolved):
            section = f"=== {p} ===\n[REJECTED: path escapes driver root]"
            section_bytes = len(section.encode("utf-8"))
            added = section_bytes + (SEP_BYTES if out else 0)
            if used + added > byte_cap:
                out.append(f"=== {p} ===\n[TRUNCATED: total {byte_cap}-byte cap reached]")
                break
            out.append(section)
            used += added
            continue
        # Code-level .git deny (defense-in-depth). When the root is a PR
        # checkout, `.git/config` can hold the persisted Actions token.
        # `persist-credentials: false` (reviewer-bot.yml) is the primary
        # control, but don't couple token safety to a single YAML line with
        # no code-level backstop: refuse to read anything under `.git`
        # outright. Mirrors `grep_tool`'s `_GREP_EXCLUDE_DIRS` pruning and the
        # followup path's `can_use_tool` `.git` deny. See PR review thread on
        # scripts/reviewer_bot/agent.py (read_paths .git guard).
        if ".git" in target.relative_to(driver_root_resolved).parts:
            section = f"=== {p} ===\n[REJECTED: .git path (refusing to read VCS metadata)]"
            section_bytes = len(section.encode("utf-8"))
            added = section_bytes + (SEP_BYTES if out else 0)
            if used + added > byte_cap:
                out.append(f"=== {p} ===\n[TRUNCATED: total {byte_cap}-byte cap reached]")
                break
            out.append(section)
            used += added
            continue
        # Symlink rejection (defense-in-depth alongside the resolved-
        # is-inside check above). `target = (driver_root / p).resolve()`
        # already follows symlinks and checks containment, so a symlink
        # to outside the root would have failed the prior check. But a
        # symlink WITHIN the root could still be unexpected — e.g., an
        # author commits a symlink to a sibling test file as obfuscation.
        # Use `lstat` via `Path.is_symlink()` on the pre-resolve `p`
        # (relative path joined to driver_root, no resolve) so we can
        # detect symlinks at the leaf. See PR #414 review r3326846644.
        link_candidate = driver_root / p
        try:
            if link_candidate.is_symlink():
                section = f"=== {p} ===\n[REJECTED: symlink (refusing to follow)]"
                section_bytes = len(section.encode("utf-8"))
                added = section_bytes + (SEP_BYTES if out else 0)
                if used + added > byte_cap:
                    out.append(f"=== {p} ===\n[TRUNCATED: total {byte_cap}-byte cap reached]")
                    break
                out.append(section)
                used += added
                continue
        except OSError:
            # If lstat fails (broken link, permission issue), treat as
            # non-existent — the next is_file() check will produce the
            # NOT FOUND section.
            pass
        if not target.is_file():
            section = f"=== {p} ===\n[NOT FOUND or not a regular file]"
            section_bytes = len(section.encode("utf-8"))
            added = section_bytes + (SEP_BYTES if out else 0)
            if used + added > byte_cap:
                out.append(f"=== {p} ===\n[TRUNCATED: total {byte_cap}-byte cap reached]")
                break
            out.append(section)
            used += added
            continue
        text = target.read_text(errors="replace")
        section = f"=== {p} ===\n{text}"
        section_bytes = len(section.encode("utf-8"))
        added = section_bytes + (SEP_BYTES if out else 0)
        if used + added > byte_cap:
            out.append(f"=== {p} ===\n[TRUNCATED: total {byte_cap}-byte cap reached]")
            break
        out.append(section)
        used += added
    return "\n\n".join(out) if out else "[no paths provided]"


# Directory names skipped by `grep_tool`'s walk. When the root is a PR
# checkout (or the cloned driver repo), `rglob("*")` otherwise descends into
# the VCS metadata and build/vendor trees, `read_text`-ing every file under
# them -- wasteful and a source of noisy `.git/` matches (packed objects,
# COMMIT_EDITMSG, refs) that can get inlined into the tool output. Excluding
# these by path component keeps the scan scoped to source. See PR review
# thread on scripts/reviewer_bot/sdk_tools.py (grep_default = ".").
_GREP_EXCLUDE_DIRS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        "node_modules",
        "__pycache__",
        ".venv",
        "venv",
        ".mypy_cache",
        ".pytest_cache",
        ".tox",
    }
)


def _walk_source_files(root: Path):
    """Yield files under `root`, pruning `_GREP_EXCLUDE_DIRS` *during* the
    walk so excluded trees are never descended into or materialized.

    Uses `os.walk` with in-place `dirnames[:]` filtering rather than
    `sorted(root.rglob("*"))`: on a `fetch-depth: 0` PR checkout the latter
    eagerly walks and lists the entire `.git` history (loose/packed objects,
    refs) plus any `node_modules`/`.venv` into one in-memory list before a
    single excluded path is dropped. Pruning `dirnames` in place stops the
    walk from entering those directories at all.

    `followlinks=False` (the default) means symlinked directories are never
    descended into. `dirnames`/`filenames` are sorted so output ordering is
    deterministic. Files are still subject to the caller's per-candidate
    symlink + containment checks; this helper only handles directory pruning.
    """
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in _GREP_EXCLUDE_DIRS)
        base = Path(dirpath)
        for name in sorted(filenames):
            yield base / name


def grep_tool(
    *,
    pattern: str,
    path: str,
    driver_root: Path,
    max_matches: int = 50,
    byte_cap: int = 40_000,
) -> str:
    """Recursive regex grep under driver_root/path. Bounded outputs.

    Symlink safety: `rglob` walks symlinked DIRECTORIES (entering them
    transparently) and `Path.is_file()` returns True for symlinks to
    files (it follows the link). Without the explicit `is_symlink()`
    skip + resolve-and-contain check below, this is exploitable when
    the root is author-controlled (PR checkout): a symlink under the
    repo pointing at `/etc/passwd` or `~/.aws/credentials` would be
    read and its contents inlined into the tool result. The initial-
    review pass already grepped the cloned driver source (trusted
    org-internal); reusing this function for the PR-checkout repo
    in `reviewer_bot.followup` widened the trust perimeter and
    required this hardening. See PR #414 review thread r3326846644.
    """
    driver_root_resolved = driver_root.resolve()
    try:
        target_root = (driver_root / path).resolve()
    except (OSError, ValueError) as e:
        return f"[REJECTED: invalid path '{path}': {e}]"
    if not target_root.is_relative_to(driver_root_resolved):
        return f"[REJECTED: path '{path}' escapes driver root]"
    if not target_root.exists():
        return f"[NOT FOUND: '{path}' does not exist]"
    try:
        regex = re.compile(pattern)
    except re.error as e:
        return f"[invalid regex: {e}]"

    matches: list[str] = []
    # Byte budget is measured in UTF-8 bytes of the full emitted entry
    # (line content + the "\n" separator that join() inserts between
    # entries). `len(entry)` is a character count and under-reports
    # non-ASCII content; matching the unit to the `byte_cap` name keeps
    # the cap predictable.
    NEWLINE_BYTES = len("\n".encode("utf-8"))
    used = 0
    # The followup schema advertises `path` as accepting either a
    # directory or a single file (e.g. `.github/workflows/foo.yml`).
    # `rglob("*")` on a file yields nothing, so without this branch a
    # single-file grep would always return "no matches" and silently
    # fail to verify the fix. See PR #414 review thread (Copilot).
    #
    # Symlink check uses the PRE-resolve path (mirrors the analogous
    # branch in `read_paths_tool`): `target_root` is already resolved,
    # so `is_symlink()` on it always returns False. Checking the
    # unresolved `driver_root / path` catches the case where the
    # author committed a symlink at the leaf (even one that resolves
    # inside the root), matching the directory-mode loop's layer-1
    # `is_symlink()` skip.
    pre_resolve = driver_root / path
    if pre_resolve.is_symlink():
        return f"[REJECTED: path '{path}' is a symlink (refusing to follow)]"
    if target_root.is_file():
        candidates = [target_root]
    else:
        # `_walk_source_files` prunes `_GREP_EXCLUDE_DIRS` *during* the walk
        # (in-place `dirnames[:]` filtering) so excluded sub-trees are never
        # descended into -- avoiding the eager `sorted(rglob("*"))`
        # materialization of the whole `.git` history on a `fetch-depth: 0`
        # PR checkout. The per-candidate check below is still required.
        candidates = _walk_source_files(target_root)
    for f in candidates:
        # Prune VCS metadata and build/vendor trees. The walk above already
        # skips excluded *sub*directories; this per-candidate check is still
        # needed for the case where the grep target itself is inside an
        # excluded tree -- e.g. `grep(path=".git")` / `.git/config` -- which
        # `os.walk`'s `dirnames` pruning can't catch (the excluded name is
        # the root, not a child). Anchor the check to the resolved repo root
        # (`driver_root_resolved`), NOT the grep target (`target_root`): a
        # repo-root-relative path still strips any CI absolute-path prefix
        # -- e.g. a `/home/runner/build/...` checkout dir -- so an excluded
        # name in the prefix can't wrongly drop the whole tree. Mirrors
        # `read_paths_tool`'s `relative_to(driver_root_resolved)` guard. This
        # avoids `read_text`-ing (and inlining matches from) `.git/` etc.
        if _GREP_EXCLUDE_DIRS.intersection(f.relative_to(driver_root_resolved).parts):
            continue
        # Skip symlinks outright (defense layer 1). Even resolved-and-
        # contained symlinks aren't worth reading — they point at the
        # same content as the resolved file, which would be reached
        # via the non-symlink path anyway.
        if f.is_symlink():
            continue
        if not f.is_file():
            continue
        # Defense layer 2: resolve the candidate and verify it's still
        # under `driver_root_resolved`. Catches any case where a parent
        # directory entry in `f` was itself a symlink that rglob entered
        # transparently — the resolved file path would then point
        # outside the root even though `is_symlink()` on `f` returns
        # False for the file itself.
        try:
            f_resolved = f.resolve()
        except OSError:
            continue
        if not f_resolved.is_relative_to(driver_root_resolved):
            continue
        try:
            text = f.read_text(errors="replace")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if regex.search(line):
                rel = f.relative_to(driver_root)
                entry = f"{rel}:{lineno}: {line.rstrip()[:200]}"
                entry_bytes = len(entry.encode("utf-8"))
                added = entry_bytes + (NEWLINE_BYTES if matches else 0)
                if used + added > byte_cap:
                    matches.append(f"[TRUNCATED: byte cap reached after {len(matches)} matches]")
                    return "\n".join(matches)
                matches.append(entry)
                used += added
                if len(matches) >= max_matches:
                    matches.append(f"[TRUNCATED: {max_matches}-match cap reached]")
                    return "\n".join(matches)
    return "\n".join(matches) if matches else f"[no matches for /{pattern}/ in {path}]"
