"""Generic HTML marker builders for bot comments.

Posting under `github-actions[bot]` is shared across all workflows in
this repo. Markers disambiguate which bot a comment belongs to. Each
bot picks a namespace ("reviewer-bot", "engineer-bot", etc.) and uses
the helpers below.
"""
from __future__ import annotations

import re

_VERSION = "v1"

_ID_ALLOWED = re.compile(r"[^A-Za-z0-9_.\-]")
_ID_MAX_LEN = 40


def _sanitize_id(value) -> str:
    s = str(value) if value is not None else ""
    s = _ID_ALLOWED.sub("", s)[:_ID_MAX_LEN]
    return s or "unknown"


def marker(namespace: str, kind: str) -> str:
    return f"<!-- {namespace}:{_VERSION} {kind} -->"


def marker_with_id(namespace: str, kind: str, id_value) -> str:
    return f"<!-- {namespace}:{_VERSION} {kind} id={_sanitize_id(id_value)} -->"


def has_marker(body: str, namespace: str, kind: str) -> bool:
    """True iff `body` contains the exact marker on an unquoted line.

    See `has_any_marker` for the rationale on the quoted-line filter
    — the same false-positive applies to specific-kind matches."""
    needle = marker(namespace, kind)
    return _scan_unquoted_lines(body, needle)


def has_any_marker(body: str, namespace: str) -> bool:
    """True iff `body` contains an active marker for `namespace`.

    "Active" means the marker appears on a line that is NOT a markdown
    blockquote (lines beginning with `>`, optionally indented). When a
    reviewer quotes a bot's prior reply via GitHub's blockquote syntax,
    the quoted text — INCLUDING the trailing HTML-comment marker —
    appears in the reviewer's reply body. A naive substring check
    misclassifies that reviewer reply as bot-authored, which suppresses
    re-engagement in `find_unaddressed_threads` and breaks the
    own-reply loop guard in both bots' followup workflows.

    Filtering out `>`-prefixed lines covers the common case. We do
    NOT (yet) parse fenced code blocks (```...```); a marker inside a
    code fence is rare in real reviewer text and the existing reply
    cap bounds the damage even if it slipped through.
    """
    if not body:
        return False
    needle = f"<!-- {namespace}:{_VERSION}"
    return _scan_unquoted_lines(body, needle)


def _scan_unquoted_lines(body: str, needle: str) -> bool:
    """Line-by-line substring scan that skips markdown blockquote
    lines (`>` prefix, with optional leading whitespace). Shared
    helper for `has_marker` and `has_any_marker`."""
    if not body:
        return False
    for line in body.splitlines():
        if line.lstrip().startswith(">"):
            continue
        if needle in line:
            return True
    return False
