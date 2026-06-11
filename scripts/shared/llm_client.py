"""Backwards-compat shim: JSON-extraction helpers relocated to json_utils.

Historically this module held the hand-rolled Databricks Model Serving client
(``call_llm``, OpenAI-format ``TOOLS_SCHEMA``) and the brace-balanced JSON
extractor. The Claude Agent SDK migration removed the transport + tool schema
— every caller now routes through ``scripts.shared.sdk_agent`` + the in-process
``@tool`` servers — and the JSON helpers moved to
``scripts/shared/json_utils.py``.

This module survives ONLY as a re-export of ``ParseError`` /
``extract_json_block`` so existing imports
(`from scripts.shared.llm_client import extract_json_block`) keep working. New
code should import from ``scripts.shared.json_utils`` directly.
"""
from __future__ import annotations

# Re-exported for backwards compatibility; canonical home is json_utils.
from scripts.shared.json_utils import (  # noqa: F401
    ParseError,
    extract_json_block,
)
