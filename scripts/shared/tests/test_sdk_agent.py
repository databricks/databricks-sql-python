"""Tests for sdk_agent endpoint translation + env configuration.

These cover the pure, SDK-independent surface of sdk_agent (the part that
does NOT require claude_agent_sdk to be installed): translate_endpoint and
configure_databricks_env. The agent-loop itself is exercised by the live
PR1 smoke test (tools/sdk_smoke.py), not here.
"""
import os

import pytest

from scripts.shared import sdk_agent


# ── translate_endpoint: derived from csharp_coverage/initial.py:_translate_endpoint ──
# (shares the v1->v2 workspace rewrite, but intentionally diverges: fail-closed
#  on empty/unrecognized input + strips a trailing /v1/messages — see below)

def test_translate_v1_invocations_url():
    src = "https://ws.azuredatabricks.net/serving-endpoints/databricks-claude-opus-4-7/invocations"
    assert sdk_agent.translate_endpoint(src) == (
        "https://ws.azuredatabricks.net/serving-endpoints/anthropic"
    )


def test_translate_already_v2_base_passthrough():
    src = "https://ws.azuredatabricks.net/serving-endpoints/anthropic"
    assert sdk_agent.translate_endpoint(src) == src


def test_translate_strips_trailing_v1_messages():
    """The CLI re-appends /v1/messages, so a base that already carries it
    must be normalised back to the bare base (avoids double-append)."""
    src = "https://ws.azuredatabricks.net/serving-endpoints/anthropic/v1/messages"
    assert sdk_agent.translate_endpoint(src) == (
        "https://ws.azuredatabricks.net/serving-endpoints/anthropic"
    )


def test_translate_strips_trailing_slash():
    src = "https://ws.azuredatabricks.net/serving-endpoints/some-model/invocations/"
    assert sdk_agent.translate_endpoint(src) == (
        "https://ws.azuredatabricks.net/serving-endpoints/anthropic"
    )


def test_translate_empty_fails_closed():
    """Empty / unparseable input must return '' (not a bogus relative
    '/serving-endpoints/anthropic') so run_agent never sets a malformed
    ANTHROPIC_BASE_URL."""
    assert sdk_agent.translate_endpoint("") == ""
    assert sdk_agent.translate_endpoint("   ") == ""
    assert sdk_agent.translate_endpoint("https://ws.net/no/serving/here") == ""


# ── configure_databricks_env ──

@pytest.fixture
def clean_env(monkeypatch):
    for k in (
        "ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_MODEL",
        "ANTHROPIC_API_KEY", "MODEL_ENDPOINT", "DATABRICKS_TOKEN",
    ):
        monkeypatch.delenv(k, raising=False)
    return monkeypatch


def test_configure_sets_all_three_anthropic_vars(clean_env):
    sdk_agent.configure_databricks_env(
        "databricks-claude-opus-4-7",
        endpoint="https://ws.net/serving-endpoints/m/invocations",
        token="dapi-secret",
    )
    assert os.environ["ANTHROPIC_BASE_URL"] == "https://ws.net/serving-endpoints/anthropic"
    assert os.environ["ANTHROPIC_AUTH_TOKEN"] == "dapi-secret"
    assert os.environ["ANTHROPIC_MODEL"] == "databricks-claude-opus-4-7"


def test_configure_never_sets_api_key(clean_env):
    """Databricks uses Bearer auth via ANTHROPIC_AUTH_TOKEN, NOT the
    ANTHROPIC_API_KEY name."""
    sdk_agent.configure_databricks_env(
        "m", endpoint="https://ws.net/serving-endpoints/m/invocations", token="t"
    )
    assert "ANTHROPIC_API_KEY" not in os.environ


def test_configure_falls_back_to_process_env(clean_env):
    clean_env.setenv("MODEL_ENDPOINT", "https://ws.net/serving-endpoints/m/invocations")
    clean_env.setenv("DATABRICKS_TOKEN", "from-env")
    sdk_agent.configure_databricks_env("m")
    assert os.environ["ANTHROPIC_BASE_URL"] == "https://ws.net/serving-endpoints/anthropic"
    assert os.environ["ANTHROPIC_AUTH_TOKEN"] == "from-env"


# ── _classify_http_error: must NOT match incidental numbers (review F1/F2) ──

def test_classify_matches_http_context():
    assert sdk_agent._classify_http_error(Exception("HTTP 429 Too Many Requests")) == 429
    assert sdk_agent._classify_http_error(Exception("status: 503")) == 503
    assert sdk_agent._classify_http_error(Exception("code=500")) == 500


def test_classify_ignores_incidental_numbers():
    """A bare 3-digit number with no HTTP/status/code context must NOT be
    mistaken for a status code (would spuriously skip the retrospective)."""
    for msg in ("connection reset after 500ms", "port 5000 unavailable",
                "line 412 in foo.py", "retry budget 4000"):
        assert sdk_agent._classify_http_error(Exception(msg)) is None


def test_classify_prefers_structured_attr():
    exc = Exception("opaque transport failure")
    exc.status_code = 502  # type: ignore[attr-defined]
    assert sdk_agent._classify_http_error(exc) == 502


# ── _merge_coding_agent_header: must never DROP our header (review F3) ──

def test_merge_header_sets_when_absent():
    env = {}
    sdk_agent._merge_coding_agent_header(env)
    assert env["ANTHROPIC_CUSTOM_HEADERS"] == sdk_agent.DATABRICKS_CODING_AGENT_HEADER


def test_merge_header_appends_to_unrelated_value():
    """A caller's pre-existing header must NOT drop the coding-agent opt-in
    (a plain setdefault would — reproducing the 400 this fixes)."""
    env = {"ANTHROPIC_CUSTOM_HEADERS": "x-trace-id: abc"}
    sdk_agent._merge_coding_agent_header(env)
    val = env["ANTHROPIC_CUSTOM_HEADERS"]
    assert "x-trace-id: abc" in val
    assert "x-databricks-use-coding-agent-mode" in val


def test_merge_header_idempotent():
    env = {"ANTHROPIC_CUSTOM_HEADERS": "x-databricks-use-coding-agent-mode: true"}
    sdk_agent._merge_coding_agent_header(env)
    assert env["ANTHROPIC_CUSTOM_HEADERS"].count("coding-agent-mode") == 1


def test_merge_header_appends_when_set_to_false():
    """A caller who set the header to `false` must still get the `: true`
    opt-in appended (presence check matches the full name: true). (Review F11/F14)"""
    env = {"ANTHROPIC_CUSTOM_HEADERS": "x-databricks-use-coding-agent-mode: false"}
    sdk_agent._merge_coding_agent_header(env)
    assert "x-databricks-use-coding-agent-mode: true" in env["ANTHROPIC_CUSTOM_HEADERS"]


def test_maybe_raise_http_error_uses_api_error_status():
    """_maybe_raise_http_error reads the structured api_error_status first
    (confirmed present on ResultMessage by the smoke probe), not just the
    message regex. (Review — doc:62)"""
    from types import SimpleNamespace
    msg = SimpleNamespace(is_error=True, api_error_status=503, result="overloaded", subtype="error")
    try:
        sdk_agent._maybe_raise_http_error(msg)
        assert False, "expected _HttpError"
    except sdk_agent._HttpError as e:
        assert e.code == 503
    # No error → no raise.
    ok = SimpleNamespace(is_error=False, api_error_status=None, result="OK")
    sdk_agent._maybe_raise_http_error(ok)  # must not raise


def test_record_tool_use_handles_mcp_qualified_names():
    """The SDK reports in-process @tool names fully-qualified
    (mcp__engineer-tools__edit_file). _record_tool_use must still populate
    touched_files / bash_invocations or the write-scope gate goes blind.
    (Review — initial.py:279)"""
    r = sdk_agent.AgentResult()
    sdk_agent._record_tool_use(r, "mcp__engineer-tools__edit_file", {"path": "tests/x.cs"})
    sdk_agent._record_tool_use(r, "mcp__engineer-tools__bash", {"command": "dotnet build"})
    assert r.touched_files == ["tests/x.cs"]
    assert r.bash_invocations == [{"cmd": "dotnet build"}]
    # Built-in (unqualified) names still work.
    r2 = sdk_agent.AgentResult()
    sdk_agent._record_tool_use(r2, "Edit", {"file_path": "a.py"})
    assert r2.touched_files == ["a.py"]


def test_maybe_raise_http_error_ignores_non_error_status():
    """A 2xx/1xx api_error_status must not mint an http_error stop."""
    from types import SimpleNamespace
    msg = SimpleNamespace(is_error=True, api_error_status=200, result="weird", subtype="")
    sdk_agent._maybe_raise_http_error(msg)  # 200 is not 4xx/5xx -> no raise (regex fallback finds nothing)


# ── Tier-2: output_format / structured_output capture ──
# A hardened live smoke probe (tools/sdk_smoke.py) confirmed the Databricks
# gateway honours output_format json_schema: a clean {'verdict':'ok',
# 'findings':[]} dict on a subtype=success terminal ResultMessage. These unit
# tests pin the sdk_agent plumbing that surfaces it to callers.

class ResultMessage:
    # Class name drives _accumulate's dispatch (cls = type(msg).__name__), so it
    # MUST be 'ResultMessage' for the terminal-message branch to fire.
    def __init__(self, **kw):
        self.__dict__.update(kw)


def test_accumulate_captures_structured_output():
    """When output_format is set, the validated dict arrives on
    ResultMessage.structured_output; _accumulate must lift it onto
    AgentResult.structured_output so the reviewer can drop finalize_review."""
    r = sdk_agent.AgentResult()
    payload = {"verdict": "ok", "findings": []}
    sdk_agent._accumulate(r, ResultMessage(structured_output=payload, subtype="success"))
    assert r.structured_output == payload


def test_accumulate_leaves_structured_output_none_when_absent():
    """Tier-1 (no output_format): the terminal message has no structured_output
    (or None), so AgentResult.structured_output stays None — existing callers
    are unaffected."""
    r = sdk_agent.AgentResult()
    assert r.structured_output is None
    sdk_agent._accumulate(r, ResultMessage(structured_output=None, subtype="success"))
    assert r.structured_output is None
    # Attribute entirely absent (older SDK / non-output_format run) is also fine.
    sdk_agent._accumulate(r, ResultMessage(subtype="success"))
    assert r.structured_output is None


# ── Real per-run telemetry (replaces the reviewer observer 0/0/(none)) ──

class ToolUseBlock:
    def __init__(self, name, **kw):
        self.name = name
        self.input = kw.get("input", {})
        self.id = kw.get("id", "t1")


class TextBlock:
    def __init__(self, text):
        self.text = text


class ThinkingBlock:
    def __init__(self, thinking):
        self.thinking = thinking


class AssistantMessage:
    def __init__(self, content):
        self.content = content


def test_accumulate_records_every_tool_call():
    """_accumulate appends EVERY ToolUseBlock name (not just edit/bash) to
    AgentResult.tool_calls, so the reviewer observer reports real tool usage
    (read_paths/grep/finalize_review) instead of a hardcoded '(none)'."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, AssistantMessage([
        ToolUseBlock("mcp__reviewer-tools__read_paths"),
        ToolUseBlock("mcp__reviewer-tools__finalize_review"),
    ]))
    assert r.tool_calls == [
        "mcp__reviewer-tools__read_paths",
        "mcp__reviewer-tools__finalize_review",
    ]


def test_format_tool_input_shows_full_values_up_to_cap():
    out = sdk_agent._format_tool_input({
        "pattern": "Lz4Buffer",
        "reason": "y" * 300,                  # well under the 500 cap
        "paths": ["a.cs", "b.cs"],
    })
    assert "pattern=Lz4Buffer" in out
    assert "y" * 300 in out               # full value (under cap), not truncated
    assert "…" not in out                 # no marker when under cap
    assert '["a.cs", "b.cs"]' in out      # list rendered fully as compact JSON


def test_format_tool_input_caps_large_value_with_explicit_marker():
    out = sdk_agent._format_tool_input({"content": "x" * 5000})
    assert "x" * 5000 not in out          # large value capped, not dumped whole
    assert "…[+4500 chars]" in out        # explicit dropped-char marker
    assert len(out) < 600                 # bounded near the 500 cap


def test_format_tool_input_flattens_newlines_to_one_line():
    out = sdk_agent._format_tool_input({"command": "dotnet build\ndotnet test"})
    assert "\n" not in out
    assert "command=dotnet build dotnet test" in out


def test_accumulate_prints_live_per_turn_trace_for_tool_turns(capsys):
    """A ToolUseBlock turn prints `[agent] turn N: tool=...` (and NOT a
    `(no tool)` line)."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, AssistantMessage([
        ToolUseBlock(
            "mcp__reviewer-tools__grep",
            input={"pattern": "foo", "path": "csharp/src/"},
        ),
    ]))
    out = capsys.readouterr().out
    assert "[agent] turn 1: tool=mcp__reviewer-tools__grep" in out
    assert "pattern=foo" in out
    assert "(no tool)" not in out


def test_accumulate_logs_text_only_turn(capsys):
    """A turn with no tool call still logs `[agent] turn N: (no tool) ...` so
    every turn is accounted for (and a slow thinking turn is visible)."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, AssistantMessage([
        TextBlock("Let me think about this before acting."),
    ]))
    out = capsys.readouterr().out
    assert "[agent] turn 1: (no tool)" in out
    assert "Let me think about this before acting." in out


def test_accumulate_logs_thinking_only_turn(capsys):
    """A thinking-only turn (ThinkingBlock, no text/tool) surfaces the thinking
    content as `(thinking) ...` instead of a blank `(no tool)` line."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, AssistantMessage([
        ThinkingBlock("First I should read the file, then check the callers."),
    ]))
    out = capsys.readouterr().out
    assert "[agent] turn 1: (thinking)" in out
    assert "First I should read the file" in out


def test_accumulate_captures_usage_and_cost():
    """A terminal ResultMessage with `usage` + `total_cost_usd` populates the
    token/cost fields the observer logs."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, ResultMessage(
        usage={"input_tokens": 1200, "output_tokens": 340},
        total_cost_usd=0.0123, subtype="success",
    ))
    assert r.prompt_tokens == 1200
    assert r.completion_tokens == 340
    assert abs(r.total_cost_usd - 0.0123) < 1e-9


def test_accumulate_telemetry_defaults_when_absent():
    """No usage/cost on the terminal message → fields stay at their 0 defaults
    (a missing-usage run, or the SDK absent in tests, is safe)."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, ResultMessage(subtype="success"))
    assert r.prompt_tokens == 0
    assert r.completion_tokens == 0
    assert r.total_cost_usd == 0.0
    assert r.tool_calls == []


def test_accumulate_sums_cache_tokens_into_prompt_tokens():
    """prompt_tokens reflects the FULL input the model processed: with prompt
    caching, input_tokens is tiny (the uncached delta) and the bulk is in
    cache_creation/cache_read. Summing all three is what makes 'did the diff
    reach the model?' answerable (a cached 6k-token prompt must not read as 3)."""
    r = sdk_agent.AgentResult()
    sdk_agent._accumulate(r, ResultMessage(
        usage={
            "input_tokens": 3,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 6000,
            "output_tokens": 800,
        },
        subtype="success",
    ))
    assert r.prompt_tokens == 6003
    assert r.completion_tokens == 800
