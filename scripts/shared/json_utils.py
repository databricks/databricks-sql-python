"""JSON-extraction helpers for parsing LLM responses.

Relocated from ``scripts/shared/llm_client.py`` as part of the Claude
Agent SDK migration (see ``docs/migration/claude-agent-sdk-migration.md``,
PR1). ``llm_client.py`` is gutted once every caller routes through the SDK;
this parse helper survives because the SDK delivers tool-call args
structured, but some response shapes (and the reviewer-bot ``finalize_review``
fallback) still need brace-balanced JSON extraction from free text.

``llm_client`` re-exports ``ParseError`` / ``extract_json_block`` from here
so existing imports keep working unchanged.
"""
from __future__ import annotations

import json
from typing import Any


class ParseError(ValueError):
    """The model's response could not be parsed into a JSON object."""


def extract_json_block(text: str) -> dict[str, Any]:
    """Extract a JSON object from the model's response.

    Robust against three common model behaviors:
      - Pure JSON output (no prose, no fence).
      - JSON wrapped in a ```json fence (legacy; the prompt no longer
        requires fences, but the model may still add them).
      - JSON containing nested ``` blocks inside string values (e.g.,
        a `suggestion` field with a ```suggestion fenced code block).
        Regex-based extraction breaks on this; brace-balancing does not.
      - Multiple top-level JSON objects in the same response (e.g.,
        the model echoes the schema example before emitting its
        actual answer). The LAST balanced object is returned.

    Raises ParseError if no parseable JSON object is found.
    """
    text = text.strip()
    if not text:
        raise ParseError("Empty response")

    # Fast path: response is pure JSON.
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    # Slow path: scan for balanced top-level {…} regions and try each
    # from last to first. String state is tracked so braces / backticks
    # inside string values don't confuse the matcher.
    candidates = _find_balanced_objects(text)
    if not candidates:
        raise ParseError("No parseable JSON object in response")

    last_err: Exception | None = None
    for start, end in reversed(candidates):
        try:
            obj = json.loads(text[start:end + 1])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError as e:
            last_err = e
            continue
    raise ParseError(
        f"All {len(candidates)} candidate JSON blocks failed to parse: "
        f"{last_err}"
    )


def _find_balanced_objects(text: str) -> list[tuple[int, int]]:
    """Return (start, end) offsets of every balanced {…} region in text.
    Tracks string state (with escape handling) so braces inside string
    values do not affect depth.
    """
    out: list[tuple[int, int]] = []
    i = 0
    n = len(text)
    while i < n:
        if text[i] == "{":
            end = _balanced_brace_end(text, i)
            if end != -1:
                out.append((i, end))
                i = end + 1
                continue
        i += 1
    return out


def _balanced_brace_end(text: str, start: int) -> int:
    """Return index of the matching '}' for the '{' at text[start], or
    -1 if unbalanced. String state tracking ignores braces inside
    JSON string literals (handles escaped quotes and backslashes)."""
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if in_string:
            if ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
    return -1
