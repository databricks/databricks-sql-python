"""Tests for the relocated JSON-extraction helpers.

These import from the new canonical home (``json_utils``); the parallel
``test_llm_client.py`` suite imports the same names via the re-export shim
in ``llm_client`` and must keep passing — together they prove the PR1
relocation is behaviour-preserving.
"""
import json

import pytest

from scripts.shared.json_utils import extract_json_block, ParseError


def test_pure_json():
    assert extract_json_block('{"a": 1, "b": [2, 3]}') == {"a": 1, "b": [2, 3]}


def test_fenced_json():
    text = "preamble\n```json\n{\"findings\": [], \"summary\": \"ok\"}\n```\ntrailer"
    assert extract_json_block(text) == {"findings": [], "summary": "ok"}


def test_nested_backticks_in_string_value():
    """A ```suggestion fenced block inside a string value must not break
    brace-balancing (the reason we use a balancer, not a regex)."""
    payload = {"suggestion": "```suggestion\nAssertEqual(5, x)\n```", "id": "F1"}
    text = f"Here is my review:\n{json.dumps(payload)}"
    assert extract_json_block(text)["suggestion"].startswith("```suggestion")


def test_last_object_wins_when_schema_echoed():
    text = '{"example": true}\nactual answer below\n{"findings": [], "summary": "real"}'
    assert extract_json_block(text) == {"findings": [], "summary": "real"}


def test_braces_inside_strings_ignored():
    assert extract_json_block('{"body": "use {curly} and \\"quoted\\" text"}') == {
        "body": 'use {curly} and "quoted" text'
    }


def test_empty_raises():
    with pytest.raises(ParseError):
        extract_json_block("   ")


def test_no_object_raises():
    with pytest.raises(ParseError):
        extract_json_block("no json here at all")


def test_reexport_identity():
    """The shim in llm_client must be the very same object, not a copy."""
    from scripts.shared import llm_client
    assert llm_client.extract_json_block is extract_json_block
    assert llm_client.ParseError is ParseError
