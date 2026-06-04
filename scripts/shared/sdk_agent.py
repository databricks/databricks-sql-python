"""SDK-backed agent runner (proof-of-concept).

Replaces the hand-rolled transport + tool-use loop in
``scripts/engineer_bot/agent_runner.py`` (POST loop, 429/transient
backoff, stop_reason contract) and ``scripts/shared/llm_client.call_llm``
(the bespoke HTTP client) with the Claude Agent SDK
(``claude_agent_sdk``). The SDK spawns the Claude Code CLI, which speaks
the same Anthropic Messages protocol the bots already POST by hand.

What this module preserves verbatim from ``agent_runner``:

  * The ``AgentResult`` dataclass shape every caller depends on
    (``transcript``, ``touched_files``, ``bash_invocations``,
    ``final_text``, ``turns``, ``stop_reason``).
  * The ``stop_reason`` contract: ``"end_turn"`` / ``"max_turns"`` /
    ``"http_error_<code>"`` (``HTTP_ERROR_STOP_PREFIX`` re-exported so
    ``csharp_coverage/initial.py``'s skip-retrospective check keeps
    working).
  * The LINEAR 30/60/90 -> 120s retry curve (we deliberately do NOT
    trust the SDK's internal backoff; ``_with_linear_retry`` wraps the
    whole run so the workflow-step timing contract survives).

What moves to the SDK: transport, the request -> tool_use -> tool_result
loop, and tool dispatch.

Endpoint feasibility (verified against ``csharp_coverage/initial.py``
``_translate_endpoint``): the SDK's CLI targets Databricks Model Serving
via ``ANTHROPIC_BASE_URL`` (the translated ``.../serving-endpoints/
anthropic`` base, to which the CLI appends ``/v1/messages``),
``ANTHROPIC_AUTH_TOKEN`` (Bearer auth from ``DATABRICKS_TOKEN`` -- NOT
``ANTHROPIC_API_KEY``), and ``ANTHROPIC_MODEL`` (model name in the
request body).

Imports of ``claude_agent_sdk`` are GUARDED: this module imports cleanly
even when the SDK is absent (so unit tests for ``translate_endpoint`` /
retry can run anywhere). The clear ImportError is raised only when
``run_agent`` is actually called.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

# Reused for the env=None default in run_agent: scrub a copy of os.environ so
# the SDK subprocess keeps PATH/HOME but loses credential-shaped vars (#5).
# Canonical home is shared/ (no dependency on engineer_bot).
from scripts.shared import env_scrub

# ── Guarded SDK import ────────────────────────────────────────────────
# Keep the module importable without the SDK installed; surface a clear
# error only at run_agent() call time.
try:  # pragma: no cover - exercised only where the SDK is installed
    from claude_agent_sdk import (
        ClaudeAgentOptions,
        ClaudeSDKClient,
    )

    _SDK_IMPORT_ERROR: Optional[Exception] = None
except Exception as _exc:  # noqa: BLE001 - any import failure is non-fatal here
    ClaudeAgentOptions = None  # type: ignore[assignment]
    ClaudeSDKClient = None  # type: ignore[assignment]
    _SDK_IMPORT_ERROR = _exc


# Stop-reason prefix emitted when an LLM-API call returns a non-2xx HTTP
# status. Re-exported with the SAME literal as
# ``agent_runner.HTTP_ERROR_STOP_PREFIX`` so consumers (notably
# ``csharp_coverage/initial.py``, which skips the retrospective on
# HTTP-error stops) detect HTTP-error stops without duplicating the
# string. If you rename it, grep ``HTTP_ERROR_STOP_PREFIX`` /
# ``http_error_`` across the repo first.
HTTP_ERROR_STOP_PREFIX = "http_error_"

# Databricks' Anthropic gateway rejects the Claude Code CLI's default
# ``anthropic-beta`` flags with ``400 {"message":"invalid beta flag"}`` UNLESS
# this header opts the request into the gateway's coding-agent mode. Confirmed
# fix from the Model Serving team (Slack #ai-devtools, 2026-01-29) and mirrors
# universe ``devtools/ai/llm-lib/agents/claudecode.py`` model-serving env. The
# CLI forwards ``ANTHROPIC_CUSTOM_HEADERS`` ("Name: value") on every request.
DATABRICKS_CODING_AGENT_HEADER = "x-databricks-use-coding-agent-mode: true"


def _merge_coding_agent_header(env: dict[str, str]) -> None:
    """Ensure ``ANTHROPIC_CUSTOM_HEADERS`` carries the coding-agent header
    (with value ``true``), MERGING with any value a caller already set rather
    than no-op'ing.

    A plain ``setdefault`` would silently drop our header if the caller set
    ``ANTHROPIC_CUSTOM_HEADERS`` for any other reason (tracing, etc.) — which
    reproduces the exact ``400 invalid beta flag`` this header exists to fix.
    The CLI parses multiple ``Name: value`` headers separated by newlines, so
    we append ours when it isn't already present.

    The presence check matches the full ``name: true`` header (not just the
    name), so a caller who set ``...coding-agent-mode: false`` still gets the
    ``: true`` opt-in appended rather than being treated as already-correct.
    """
    existing = (env.get("ANTHROPIC_CUSTOM_HEADERS") or "").strip()
    if DATABRICKS_CODING_AGENT_HEADER.lower() in existing.lower():
        return
    env["ANTHROPIC_CUSTOM_HEADERS"] = (
        f"{existing}\n{DATABRICKS_CODING_AGENT_HEADER}" if existing
        else DATABRICKS_CODING_AGENT_HEADER
    )


@dataclass
class AgentResult:
    """Drop-in shape for ``agent_runner.AgentResult``.

    Field names + semantics are identical so callers
    (``csharp_coverage/initial.py``, ``followup.py``) need no changes
    beyond the import site.
    """

    transcript: list[dict[str, Any]] = field(default_factory=list)
    touched_files: list[str] = field(default_factory=list)
    bash_invocations: list[dict[str, Any]] = field(default_factory=list)
    final_text: str = ""
    turns: int = 0
    stop_reason: str = ""
    # Tier-2: the schema-validated payload from ResultMessage.structured_output
    # when the caller passes output_format=... to run_agent; None otherwise so
    # existing (Tier-1) callers are unaffected.
    structured_output: Optional[dict[str, Any]] = None
    # Real per-run telemetry (replaces the reviewer observer's hardcoded
    # 0/0/(none) placeholders). tool_calls = every ToolUseBlock name across the
    # run; tokens + cost come from the terminal ResultMessage. All default to
    # empty/0 so a run with no usage data (or the SDK absent in tests) is safe.
    tool_calls: list[str] = field(default_factory=list)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_cost_usd: float = 0.0


class _HttpError(Exception):
    """Internal: an HTTP non-2xx surfaced by the SDK transport.

    Carries the numeric status so ``run_agent`` can re-emit the
    ``http_error_<code>`` stop_reason contract.
    """

    def __init__(self, code: int, detail: str = ""):
        super().__init__(f"HTTP {code}: {detail}"[:300])
        self.code = code


def translate_endpoint(endpoint: str) -> str:
    """Translate a v1 ``.../serving-endpoints/<model>/invocations`` URL
    into the v2 ``.../serving-endpoints/anthropic`` base URL the SDK's
    CLI uses as ``ANTHROPIC_BASE_URL`` (the CLI appends ``/v1/messages``).

    Derived from ``csharp_coverage/initial.py:_translate_endpoint`` (same
    v1->v2 workspace rewrite, so the existing ``MODEL_ENDPOINT`` workflow
    input keeps working as the sole source of the base URL), with two
    DELIBERATE additions that the original does not have: (1) fail-closed —
    return "" for empty or unrecognized input rather than a bogus relative
    URL; (2) strip a trailing ``/v1/messages`` from an already-v2 base so the
    CLI's own ``/v1/messages`` append doesn't double up. Not strict parity.
    """
    endpoint = endpoint.rstrip("/")
    if not endpoint:
        # Fail closed: an empty input must NOT produce a bogus relative
        # '/serving-endpoints/anthropic' (which run_agent would then set as
        # ANTHROPIC_BASE_URL and only discover as a confusing transport error).
        return ""
    if "/serving-endpoints/anthropic" in endpoint:
        # Already a v2 base (possibly with a trailing /v1/messages the
        # CLI would otherwise double-append) -- normalise to the base.
        return endpoint.split("/v1/messages")[0]
    if "/serving-endpoints/" not in endpoint:
        # Can't locate the workspace prefix; fail closed rather than emit a
        # malformed base URL.
        return ""
    workspace = endpoint.split("/serving-endpoints/")[0]
    return f"{workspace}/serving-endpoints/anthropic"


def configure_databricks_env(model: str, *, endpoint: str = "", token: str = "") -> None:
    """Set the ``ANTHROPIC_*`` env vars the SDK's CLI reads to target the
    Databricks Anthropic gateway.

    * ``ANTHROPIC_BASE_URL``  <- ``translate_endpoint(endpoint)`` (falls
      back to ``$MODEL_ENDPOINT`` when ``endpoint`` is empty).
    * ``ANTHROPIC_AUTH_TOKEN`` <- ``token`` or ``$DATABRICKS_TOKEN``
      (Bearer auth; NOT ``ANTHROPIC_API_KEY``).
    * ``ANTHROPIC_MODEL``     <- ``model`` (sent in the request body).

    WARNING: this mutates the PROCESS env, so the auth token becomes
    visible to any subprocess the caller later spawns. ``run_agent`` does
    NOT use this -- it scopes the ANTHROPIC_* vars to the SDK subprocess via
    ``ClaudeAgentOptions(env=...)`` so the token never enters os.environ
    (env_scrub invariant #5). This helper is retained for the standalone
    smoke script (tools/sdk_smoke.py) and local one-shot use, where the
    process is short-lived and spawns nothing else.
    """
    src_endpoint = endpoint or os.environ.get("MODEL_ENDPOINT", "")
    if src_endpoint:
        base_url = translate_endpoint(src_endpoint)
        # Only set when translation produced a usable base; an unrecognized
        # endpoint yields "" (fail-closed) and must NOT clobber a previously
        # correct ANTHROPIC_BASE_URL with an empty value.
        if base_url:
            os.environ["ANTHROPIC_BASE_URL"] = base_url
    auth = token or os.environ.get("DATABRICKS_TOKEN", "")
    if auth:
        os.environ["ANTHROPIC_AUTH_TOKEN"] = auth  # NOT ANTHROPIC_API_KEY
    if model:
        os.environ["ANTHROPIC_MODEL"] = model
        # Map Claude Code's "opus" tier to the configured Databricks model so
        # internal opus references resolve. (sonnet/haiku aliases can be added
        # if the CLI's background tasks need them.)
        os.environ.setdefault("ANTHROPIC_DEFAULT_OPUS_MODEL", model)
    # Required so the gateway accepts the CLI's anthropic-beta flags (see
    # DATABRICKS_CODING_AGENT_HEADER). Merge (don't setdefault) so a caller's
    # pre-existing ANTHROPIC_CUSTOM_HEADERS can't drop our coding-agent header.
    _merge_coding_agent_header(os.environ)  # type: ignore[arg-type]


# ── Linear retry wrapper (OUR curve, not the SDK's) ───────────────────
def _with_linear_retry(
    fn: Callable[[], Any],
    *,
    max_retries: int = 3,
) -> Any:
    """Wrap a callable in the LINEAR backoff that ``agent_runner.post_message``
    used. It is faithfully TWO curves, not one (this matches the original
    verbatim; the floor on the 429 path is deliberate):

      * 429:               min(max(60, 30*(attempt+1)), 120) -> 60, 60, 90, 120
      * transient network: min(30*(attempt+1), 120)          -> 30, 60, 90, 120

    The SDK owns its own (almost certainly exponential) backoff; this
    wrapper sits *outside* it so the workflow-step timeout expectations
    that were tuned to these curves survive the migration. 429s and
    transport errors surfaced by the SDK are retried here; an
    ``_HttpError`` with a non-429 code propagates immediately (the caller
    turns it into the ``http_error_<code>`` stop_reason). NOTE: the SDK's
    real transport-exception types are pinned by PR1's smoke probe; until
    then the ``except`` below catches the broad transport base classes.

    INTENTIONAL DIVERGENCE: ``agent_runner.post_message`` also honoured a
    ``Retry-After`` header on 429s (within the 120s clamp). This wrapper sits
    outside the SDK's HTTP request and never sees response headers, so it
    cannot honour ``Retry-After`` — it always uses the linear curve above.
    """
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except _HttpError as e:
            if e.code != 429 or attempt >= max_retries:
                raise
            wait = min(max(60, 30 * (attempt + 1)), 120)
            print(
                f"  HTTP 429; retrying in {wait}s "
                f"(attempt {attempt + 1}/{max_retries})",
                file=sys.stderr,
            )
            time.sleep(wait)
        except (TimeoutError, OSError, ConnectionError) as e:
            # Transient network failure (DNS, connection refused, socket
            # timeout). 30/60/90->120s curve (no 60s floor -- this mirrors
            # agent_runner's transient-network path, which differs from its
            # 429 path on purpose). Re-raise when exhausted.
            if attempt >= max_retries:
                raise
            wait = min(30 * (attempt + 1), 120)
            print(
                f"  Network error ({type(e).__name__}: {e}); retrying in "
                f"{wait}s (attempt {attempt + 1}/{max_retries})",
                file=sys.stderr,
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


def _classify_http_error(exc: BaseException) -> Optional[int]:
    """Best-effort extraction of an HTTP status code from an SDK-surfaced
    exception, so we can honour the ``http_error_<code>`` contract.

    The SDK wraps transport errors in its own exception types; we probe
    common attributes (``status_code``, ``code``, ``status``) and a
    parenthetical ``(HTTP 429)`` / ``status 503`` substring in the
    message. Returns None when no status can be inferred.
    """
    for attr in ("status_code", "code", "status", "http_status"):
        val = getattr(exc, attr, None)
        if isinstance(val, int) and 100 <= val <= 599:
            return val
    # Message fallback: ONLY match a 4xx/5xx that appears in an explicit
    # HTTP-status context (e.g. "HTTP 429", "status: 503", "code=500").
    # A bare ``\b([45]\d{2})\b`` would match incidental numbers ("after
    # 500ms", "port 5000", "line 412") and mis-stamp arbitrary runtime
    # errors as ``http_error_<code>`` -- which csharp_coverage/initial.py
    # uses to skip the retrospective (invariant #12). PR1's smoke probe
    # pins the SDK's real exception attrs; until then this stays strict so
    # genuine programmer errors (TypeError/AttributeError from API drift)
    # fall through and re-raise rather than being silently swallowed.
    msg = str(exc)
    m = re.search(r"(?:HTTP|status|code)[\s:=/]*([45]\d{2})\b", msg, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


# ── Accumulation: rebuild AgentResult from the SDK message stream ─────
# Per-value cap for the live trace. Deliberately high so reason/pattern/path/
# command and most thinking render in full (the 60-char cap was too tight),
# while a large edit_file body / full review payload is still bounded instead
# of dumping unbounded content into CI logs on every turn.
_TRACE_VALUE_CAP = 500


def _truncate_trace_value(s: str, cap: int = _TRACE_VALUE_CAP) -> str:
    """Cap a rendered trace value, appending an explicit ``…[+N chars]`` marker
    so a truncation is never silent (you always see how much was dropped)."""
    return s if len(s) <= cap else s[:cap] + f"…[+{len(s) - cap} chars]"


def _format_tool_input(tool_input: dict[str, Any]) -> str:
    """Render a tool_use input dict as a single trace line: ``key=value`` for
    each arg. Values render in full up to ``_TRACE_VALUE_CAP`` (then a
    ``…[+N chars]`` marker); newlines are flattened so each turn stays one
    greppable log line; lists/dicts are rendered as compact JSON."""
    parts: list[str] = []
    for k, v in (tool_input or {}).items():
        if isinstance(v, (list, tuple, dict)):
            s = json.dumps(v, default=str)
        else:
            s = str(v)
        parts.append(f"{k}={_truncate_trace_value(s.replace(chr(10), ' '))}")
    return " ".join(parts)


def _accumulate(result: AgentResult, msg: Any) -> None:
    """Fold one SDK message into ``result``.

    Mirrors ``agent_runner.run_agent``'s bookkeeping:
      * each assistant turn increments ``turns`` and appends to
        ``transcript``;
      * the final assistant text becomes ``final_text``;
      * ``touched_files`` is derived from ``Edit`` / ``Write`` / our
        ``edit_file`` tool-use blocks;
      * ``bash_invocations`` is derived from ``Bash`` / our ``bash``
        tool-use blocks.

    The SDK delivers structured content blocks, so the manual
    ``content[*]["type"] == "tool_use"`` parsing is replaced by isinstance
    checks against the SDK block classes. We import them lazily and fall
    back to duck-typing on ``.__class__.__name__`` so this stays robust
    across SDK versions and importable without the SDK.
    """
    cls = type(msg).__name__

    if cls == "AssistantMessage":
        result.turns += 1
        blocks = getattr(msg, "content", []) or []
        text_parts: list[str] = []
        thinking_parts: list[str] = []
        transcript_blocks: list[dict[str, Any]] = []
        for block in blocks:
            bname = type(block).__name__
            if bname == "TextBlock":
                txt = getattr(block, "text", "") or ""
                text_parts.append(txt)
                transcript_blocks.append({"type": "text", "text": txt})
            elif bname in ("ThinkingBlock", "RedactedThinkingBlock"):
                # Extended-thinking turns deliver a ThinkingBlock (no TextBlock,
                # no ToolUseBlock). Capture the thinking text for the live trace
                # so a thinking-only turn isn't a blank `(no tool)` line. NOT
                # added to transcript_blocks — keep the transcript shape (and the
                # retrospective/observer that read it) byte-for-byte unchanged.
                think = (
                    getattr(block, "thinking", "")
                    or getattr(block, "text", "")
                    or ""
                )
                if think:
                    thinking_parts.append(think)
            elif bname == "ToolUseBlock":
                tool_name = getattr(block, "name", "") or ""
                tool_input = getattr(block, "input", {}) or {}
                # Preserve the block ``id`` so downstream consumers
                # (retrospective compaction) can pair tool_use with the
                # matching tool_result via id/tool_use_id, exactly as the
                # agent_runner transcript did.
                transcript_blocks.append(
                    {
                        "type": "tool_use",
                        "id": getattr(block, "id", None),
                        "name": tool_name,
                        "input": tool_input,
                    }
                )
                _record_tool_use(result, tool_name, tool_input)
                # Live per-turn trace (shared by every bot/entrypoint that
                # routes through run_agent). The agent loop is otherwise
                # opaque between the pre-agent "gather" logs and the final
                # result; this prints which tool each turn requested. GHA
                # timestamps each log line, so a long gap before one line
                # signals a stalled model call, while many lines in quick
                # succession signal looping toward max_turns.
                print(
                    f"[agent] turn {result.turns}: tool={tool_name} "
                    f"{_format_tool_input(tool_input)}",
                    flush=True,
                )
                # Record EVERY tool name (not just edit/bash) so the reviewer's
                # observer can report real tool usage (read_paths / grep /
                # finalize_review) instead of a hardcoded "(none)".
                if tool_name:
                    result.tool_calls.append(tool_name)
        if not any(b.get("type") == "tool_use" for b in transcript_blocks):
            # No tool call this turn — still log it so every turn N is accounted
            # for and a slow turn shows as a timestamp gap + a line, not silence.
            # Prefer visible text; otherwise surface the extended-thinking
            # content (what an otherwise-blank reasoning turn actually carries).
            if text_parts:
                _line = "(no tool) " + _truncate_trace_value(
                    " ".join(text_parts).replace("\n", " ")
                )
            elif thinking_parts:
                _line = "(thinking) " + _truncate_trace_value(
                    " ".join(thinking_parts).replace("\n", " ")
                )
            else:
                _line = "(no tool, no text)"
            print(f"[agent] turn {result.turns}: {_line}", flush=True)
        if text_parts:
            # Last assistant text wins as final_text (matches the
            # agent_runner contract: final turn's text completes the run).
            result.final_text = "\n".join(text_parts)
        result.transcript.append({"role": "assistant", "content": transcript_blocks})

    elif cls == "UserMessage":
        # tool_result turns come back as UserMessage with ToolResultBlocks.
        result.transcript.append(
            {"role": "user", "content": _summarize_tool_results(msg)}
        )

    elif cls == "ResultMessage":
        # Terminal message: carries the SDK's stop reason + turn count (mapped
        # onto our contract in run_agent), AND — when the caller set
        # output_format=json_schema — the schema-validated `structured_output`
        # dict. Capture it so a caller can read the canonical result without a
        # terminator tool (Tier-2: reviewer finalize_review replacement). Absent
        # / None when output_format was not requested, leaving Tier-1 untouched.
        so = getattr(msg, "structured_output", None)
        if so is not None:
            result.structured_output = so
        # Real usage + cost for the reviewer observer (replaces 0/0 placeholders).
        # `usage` is the Anthropic usage shape; tolerate a dict or an attr object,
        # and a missing field. Cost is the SDK's own rollup. Left at 0 when the
        # gateway omits them.
        usage = getattr(msg, "usage", None)
        if usage is not None:
            def _u(key: str) -> int:
                v = usage.get(key) if isinstance(usage, dict) else getattr(usage, key, None)
                return int(v) if isinstance(v, (int, float)) else 0
            # prompt_tokens must reflect the FULL input the model processed, not
            # just the uncached delta. With prompt caching the bulk of a large
            # review prompt lands in cache_creation/cache_read, leaving
            # input_tokens tiny (e.g. 3) — which falsely read as "the model saw
            # almost nothing." Sum all three so the number answers "did the
            # diff reach the model?".
            result.prompt_tokens = (
                _u("input_tokens")
                + _u("cache_creation_input_tokens")
                + _u("cache_read_input_tokens")
            )
            result.completion_tokens = _u("output_tokens")
        cost = getattr(msg, "total_cost_usd", None)
        if isinstance(cost, (int, float)):
            result.total_cost_usd = float(cost)


def _record_tool_use(result: AgentResult, name: str, tool_input: dict) -> None:
    """Track touched_files / bash_invocations from a tool_use block.

    Handles both the SDK built-in tool names (``Edit`` / ``Write`` /
    ``Bash``) and our in-process ``@tool`` names (``edit_file`` /
    ``bash``), since a given migration phase may use either surface.

    SEMANTIC NOTE (differs from agent_runner): this fires at the model's
    tool-*request* time (the ToolUseBlock on the AssistantMessage), BEFORE
    execution. So ``bash_invocations`` records *attempted* commands -- it may
    include a command that ``can_use_tool`` / the allowlist hook then denied,
    and it carries no exit code / stdout / stderr (unlike agent_runner, which
    captured post-execution). ``touched_files`` is unaffected in practice (an
    Edit the hook denied never reaches the post-run gates because the working
    tree is unchanged).

    TODO(PR2): when ``csharp_coverage`` actually consumes ``bash_invocations``,
    move this capture into a PostToolUse hook to record EXECUTED commands +
    exit code/stdout/stderr (restoring the agent_runner contract + invariant
    #14); the design doc §3.1 is reconciled there."""
    # Normalize the MCP-qualified name (e.g. ``mcp__engineer-tools__edit_file``)
    # down to its bare tool name so the in-process @tool surface is matched
    # alongside the SDK built-ins. Without this, touched_files / bash_invocations
    # stay EMPTY for the engineer @tool path — the write-scope gate reads
    # touched_files, so a missed edit would slip past it. (Review)
    name = name.rsplit("__", 1)[-1]
    edit_like = {"Edit", "Write", "edit_file"}
    bash_like = {"Bash", "bash"}
    if name in edit_like:
        path = (
            tool_input.get("file_path")
            or tool_input.get("path")
            or ""
        )
        if path and path not in result.touched_files:
            result.touched_files.append(path)
    elif name in bash_like:
        result.bash_invocations.append(
            {"cmd": tool_input.get("command") or tool_input.get("cmd") or ""}
        )


def _stringify_content(content: Any) -> str:
    """Coerce an SDK ToolResultBlock ``content`` to a string.

    The agent_runner transcript stored tool_result ``content`` as a string
    (the tool's JSON-encoded return). The SDK may hand back a non-string
    (a list of content blocks, or a dict); downstream consumers treat a
    non-string as an error/fallback path, so we normalise here.
    """
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    try:
        return json.dumps(content)
    except (TypeError, ValueError):
        return str(content)


def _summarize_tool_results(msg: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for block in getattr(msg, "content", []) or []:
        if type(block).__name__ == "ToolResultBlock":
            out.append(
                {
                    "type": "tool_result",
                    "tool_use_id": getattr(block, "tool_use_id", None),
                    "content": _stringify_content(getattr(block, "content", None)),
                }
            )
    return out


def run_agent(
    *,
    system: str,
    prompt: str,
    model: str,
    cwd: str,
    allowed_tools: list[str],
    max_turns: int = 25,
    endpoint: str = "",
    token: str = "",
    pre_tool_hooks: Optional[Any] = None,
    can_use_tool: Optional[Callable[..., Awaitable[Any]]] = None,
    mcp_servers: Optional[dict[str, Any]] = None,
    env: Optional[dict[str, str]] = None,
    output_format: Optional[dict[str, Any]] = None,
) -> AgentResult:
    """Drive the Claude Agent SDK and return an ``AgentResult``.

    Drop-in replacement for ``agent_runner.run_agent`` /
    ``reviewer_bot.agent.run_agent``: same return shape and ``stop_reason``
    contract, the loop + transport delegated to the SDK.

    Parameters
    ----------
    system, prompt:
        System prompt + first user message.
    model:
        Model name (goes in the request body via ``ANTHROPIC_MODEL``).
    cwd:
        Working directory == the sandbox root (invariant #35: working-tree
        isolation). The SDK runs tools relative to this.
    allowed_tools:
        Explicit allowlist (invariant #39: default-deny tool surface).
        e.g. ``["mcp__engineer-tools__read_file", "Bash"]``.
    max_turns:
        Turn cap; maps to ``ClaudeAgentOptions(max_turns=...)`` and the
        ``max_turns`` stop_reason.
    pre_tool_hooks:
        ``HookMatcher`` list / dict for PreToolUse / PostToolUse (the
        bash allowlist + timeout clamp from ``sdk_security``).
    can_use_tool:
        Per-call permission callback (the safe_path + bash allowlist hard
        deny from ``sdk_security``).
    mcp_servers:
        In-process ``@tool`` MCP servers (the truncation/exact-once tool
        bodies reused from ``tools.py`` / reviewer ``sdk_tools.py``).
    env:
        Scrubbed environment (``sdk_security.scrubbed_env()``) -- the
        ``env_scrub`` invariant (#5). Passed to ``ClaudeAgentOptions(env=)``
        so spawned Bash subprocesses never see credential vars.

    Raises
    ------
    RuntimeError:
        If ``claude_agent_sdk`` is not installed (clear, late error).
    """
    if _SDK_IMPORT_ERROR is not None or ClaudeAgentOptions is None:
        raise RuntimeError(
            "claude_agent_sdk is not installed. Install it "
            "(`pip install claude-agent-sdk`) and ensure the Claude Code "
            "CLI is on PATH (`npm i -g @anthropic-ai/claude-code`). "
            f"Original import error: {_SDK_IMPORT_ERROR!r}"
        )

    # Build the SDK subprocess env WITHOUT mutating the parent os.environ.
    # ``env`` is the caller's scrubbed environment (sdk_security.scrubbed_env)
    # -- no credential-shaped vars. We add the ANTHROPIC_* auth vars to a
    # COPY that is handed to the SDK subprocess via ClaudeAgentOptions(env=)
    # only, so the token never lands in the parent process environment where
    # an unrelated subprocess the caller spawns could inherit it (env_scrub
    # invariant #5). The standalone smoke script still uses the mutating
    # configure_databricks_env() for convenience; run_agent does not.
    #
    # env=None default: scrub a COPY of os.environ rather than start from {}.
    # The SDK spawns the Claude Code CLI (a Node binary) with this env, so it
    # MUST retain PATH/HOME/NODE_PATH/etc. -- an empty base would leave the CLI
    # unable to locate `node`/`claude`. Scrubbing os.environ keeps those while
    # still stripping credential-shaped vars (invariant #5), so the safe path
    # is also the default path.
    agent_env = dict(env) if env is not None else env_scrub.scrub(dict(os.environ))
    base_url = translate_endpoint(endpoint or os.environ.get("MODEL_ENDPOINT", ""))
    if base_url:
        agent_env["ANTHROPIC_BASE_URL"] = base_url
    auth = token or os.environ.get("DATABRICKS_TOKEN", "")
    if auth:
        agent_env["ANTHROPIC_AUTH_TOKEN"] = auth  # NOT ANTHROPIC_API_KEY
    if model:
        agent_env["ANTHROPIC_MODEL"] = model
        agent_env.setdefault("ANTHROPIC_DEFAULT_OPUS_MODEL", model)
    # Opt into the gateway's coding-agent mode so it accepts the CLI's
    # anthropic-beta flags (else: 400 "invalid beta flag"). Merge so a
    # caller-supplied ANTHROPIC_CUSTOM_HEADERS can't drop our header.
    _merge_coding_agent_header(agent_env)

    opts = ClaudeAgentOptions(
        system_prompt=system,
        model=model,
        max_turns=max_turns,
        cwd=cwd,
        allowed_tools=allowed_tools,  # explicit allowlist; default-deny
        disallowed_tools=[],
        permission_mode="acceptEdits",  # non-interactive; CI-safe
        can_use_tool=can_use_tool,  # safe_path + bash allowlist (hard deny)
        hooks=pre_tool_hooks,  # PreToolUse(Bash) allowlist + timeout clamp
        mcp_servers=mcp_servers or {},  # in-process @tool servers
        env=agent_env,  # scrubbed caller env + ANTHROPIC_* (subprocess-scoped)
        setting_sources=[],  # do NOT inherit local settings / MCP (invariant #39)
        # Tier-2 opt-in: a json_schema makes the SDK force a synthetic
        # StructuredOutput tool and expose the validated dict on the terminal
        # ResultMessage.structured_output (captured in _accumulate). None (the
        # default) == no structured output, so Tier-1 callers are unchanged.
        # Confirmed honoured by the Databricks gateway via tools/sdk_smoke.py.
        output_format=output_format,
    )

    def _run_once() -> AgentResult:
        # Build a FRESH AgentResult per attempt. _with_linear_retry re-runs
        # this whole callable on a 429/transport error, so accumulating into
        # a result constructed outside would double-count transcript entries,
        # turns, and bash_invocations from the failed attempt (inflating the
        # post-run-gate input for the write-scope check). The
        # legacy agent_runner retried the single HTTP POST, never the loop,
        # so it never hit this; rebuilding here restores that semantics.
        res = AgentResult()
        # Seed the transcript with the original user prompt so the shape
        # matches agent_runner's [user, assistant, user(tool_results), ...]
        # contract that existing transcript consumers expect.
        res.transcript.append({"role": "user", "content": prompt})

        async def _drive() -> None:
            async with ClaudeSDKClient(options=opts) as client:
                await client.query(prompt)
                async for msg in client.receive_response():
                    if type(msg).__name__ == "ResultMessage":
                        _maybe_raise_http_error(msg)
                    _accumulate(res, msg)

        asyncio.run(_drive())
        return res

    try:
        result = _with_linear_retry(_run_once)
    except _HttpError as e:
        return AgentResult(stop_reason=f"{HTTP_ERROR_STOP_PREFIX}{e.code}")
    except Exception as e:  # noqa: BLE001 - map transport errors to the contract
        code = _classify_http_error(e)
        if code is not None:
            return AgentResult(stop_reason=f"{HTTP_ERROR_STOP_PREFIX}{code}")
        raise

    result.stop_reason = "max_turns" if result.turns >= max_turns else "end_turn"
    return result


def _maybe_raise_http_error(result_msg: Any) -> None:
    """Inspect a terminal ``ResultMessage`` for an error subtype and, if it
    looks like a transport HTTP error, raise ``_HttpError`` so the
    ``http_error_<code>`` contract is honoured.

    The SDK reports a terminal error via ``is_error`` plus an
    ``api_error_status`` int (confirmed present on ResultMessage by the PR1
    smoke probe). We read that attribute FIRST; only if it's absent do we fall
    back to scraping a 4xx/5xx code from the ``result``/``subtype`` text.
    Non-HTTP errors fall through (the run ends with end_turn/max_turns).
    """
    if not getattr(result_msg, "is_error", False):
        return
    detail = str(getattr(result_msg, "result", "") or getattr(result_msg, "subtype", ""))
    # Prefer the structured status the gateway surfaces over the text regex.
    # Only 4xx/5xx are HTTP errors (we're already inside the is_error branch,
    # but constrain the range so a stray 1xx/2xx can't mint an http_error stop).
    status = getattr(result_msg, "api_error_status", None)
    if isinstance(status, int) and 400 <= status <= 599:
        raise _HttpError(status, detail)
    code = _classify_http_error(Exception(detail))
    if code is not None:
        raise _HttpError(code, detail)
