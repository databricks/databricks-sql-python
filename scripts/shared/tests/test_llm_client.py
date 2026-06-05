import json
import pytest

from scripts.shared.llm_client import (
    extract_json_block, ParseError,
)


def test_extract_json_block_simple():
    text = '''Some preamble.

```json
{"findings": [], "summary": "ok"}
```

Trailing chatter.'''
    obj = extract_json_block(text)
    assert obj == {"findings": [], "summary": "ok"}


def test_extract_json_with_markdown_content():
    """Body field contains raw `<`, `>`, `&` — JSON natively handles it."""
    payload = {
        "findings": [
            {
                "id": "F1",
                "severity": "high",
                "body": "Should use `<details>` & not `<summary>` here.",
            }
        ],
        "summary": "Use **bold** and `code` & <em>html</em>."
    }
    text = f"```json\n{json.dumps(payload)}\n```"
    obj = extract_json_block(text)
    assert obj["findings"][0]["body"].startswith("Should use `<details>`")
    assert "<em>html</em>" in obj["summary"]


def test_extract_json_without_fence_works():
    """Bare JSON (no fence) now works — the prompt no longer requires
    a fence, and the parser is tolerant of either form."""
    text = '{"this": "is not in a fence", "ok": true}'
    obj = extract_json_block(text)
    assert obj == {"this": "is not in a fence", "ok": True}


def test_extract_json_with_malformed_payload_raises():
    text = "```json\n{not actually valid json\n```"
    with pytest.raises(ParseError):
        extract_json_block(text)


def test_extract_json_no_braces_at_all_raises():
    text = "the model emitted only prose, no json at all"
    with pytest.raises(ParseError):
        extract_json_block(text)


def test_extract_json_with_multiple_fences_takes_last():
    """When the model echoes the schema example before its real answer,
    the LAST balanced object is the actual response."""
    text = """```json
{"a": 1}
```
some text
```json
{"b": 2}
```"""
    obj = extract_json_block(text)
    assert obj == {"b": 2}


def test_extract_json_with_nested_suggestion_fence_in_string():
    """A finding's suggestion field contains a ```suggestion fence inside
    the string value. Brace-balanced parsing must NOT treat the inner
    ``` as the end of the outer JSON fence. This is the regression that
    broke the bot on commit 32246c95."""
    payload = {
        "findings": [
            {
                "id": "F1",
                "severity": "high",
                "body": "Replace this loop with a comprehension.",
                "suggestion": "```suggestion\nresult = [x * 2 for x in items]\n```",
            }
        ],
        "summary": "One inline suggestion."
    }
    # Wrap in a ```json fence (typical model output) — the nested
    # ```suggestion inside the suggestion string MUST not confuse the parser.
    text = f"```json\n{json.dumps(payload)}\n```"
    obj = extract_json_block(text)
    assert obj["findings"][0]["id"] == "F1"
    assert "```suggestion" in obj["findings"][0]["suggestion"]


def test_extract_json_pure_no_fence_no_prose():
    """When the model follows the new instruction (no fence, no prose,
    just JSON), the fast path returns directly."""
    text = '{"findings": [], "summary": "all good"}'
    obj = extract_json_block(text)
    assert obj == {"findings": [], "summary": "all good"}


# NOTE: the TOOLS_SCHEMA tests were removed in PR6 — the OpenAI-format
# TOOLS_SCHEMA (and call_llm) were deleted from llm_client when the bots moved
# to the SDK's in-process @tool servers. The finalize_review/read_paths/grep
# schemas now live as @tool definitions in scripts/reviewer_bot/sdk_tools.py.
