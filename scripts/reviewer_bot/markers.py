"""Hidden HTML markers used to identify the bot's own comments.

Posting under `github-actions[bot]` is shared across all workflows in
this repo. The marker disambiguates: only comments containing the
exact v1 marker string belong to this bot and are safe to delete /
patch on re-runs.
"""
from __future__ import annotations

import re

SUMMARY_MARKER = "<!-- pr-review-bot:v1 type=summary -->"
INLINE_MARKER_PREFIX = "<!-- pr-review-bot:v1 type=inline"

# Phase 2 markers — Update 1 (reconciliation) and Update 2 (soft follow-up).
# Both are appended to bot-generated replies on inline review threads so
# future runs can distinguish the bot's ORIGINAL finding (which carries
# INLINE_MARKER_PREFIX) from its own conversational replies (which carry
# one of the markers below). Filtering is marker-based, not login-based:
# the PR author may itself be `github-actions[bot]` (e.g., when reviewing
# the coverage-author bot's PRs), so login filters give wrong answers.
FOLLOWUP_MARKER_PREFIX = "<!-- pr-review-bot:v1 followup"
RECONCILE_MARKER = "<!-- pr-review-bot:v1 reconcile -->"
# Generic "any bot v1 comment" marker prefix — used to detect ANY bot
# comment when scanning thread roots in reconcile / followup. We then
# disambiguate via has_followup_marker / has_reconcile_marker.
BOT_V1_MARKER_PREFIX = "<!-- pr-review-bot:v1"


# Finding ids come from model output; sanitize before interpolating into
# an HTML comment so a malicious / malformed id can't break the marker
# (e.g. by injecting `-->`, newlines, or extra HTML).
_ID_ALLOWED = re.compile(r"[^A-Za-z0-9_.\-]")
_ID_MAX_LEN = 40


def _sanitize_finding_id(finding_id: str) -> str:
    """Strip anything that could break the HTML comment, cap length.

    - Coerce to str (model could emit int/None/etc.).
    - Drop CR/LF and any character outside [A-Za-z0-9_.-].
    - Cap at 40 chars (ids are expected to be short tokens like "F12").
    - Fall back to "unknown" on empty result.
    """
    s = str(finding_id) if finding_id is not None else ""
    s = _ID_ALLOWED.sub("", s)[:_ID_MAX_LEN]
    return s or "unknown"


def inline_marker_for(finding_id: str) -> str:
    """Marker for a specific finding's inline comment.

    The id is sanitized — model-provided ids can't break the marker
    syntax even if they contain `-->`, newlines, or arbitrary HTML.
    """
    return f"<!-- pr-review-bot:v1 type=inline id={_sanitize_finding_id(finding_id)} -->"


def has_summary_marker(body: str) -> bool:
    return SUMMARY_MARKER in body


def has_inline_marker(body: str) -> bool:
    return INLINE_MARKER_PREFIX in body


def has_followup_marker(body: str) -> bool:
    """True iff the body is a bot follow-up reply (Update 2)."""
    return FOLLOWUP_MARKER_PREFIX in body


def has_reconcile_marker(body: str) -> bool:
    """True iff the body is a bot reconcile reply (Update 1)."""
    return RECONCILE_MARKER in body


def is_bot_original_finding(body: str) -> bool:
    """True iff the body is one of the bot's ORIGINAL inline findings.

    We use this to filter open review threads down to threads that the
    bot opened with a finding. The bot's own follow-up replies and
    reconcile replies also carry pr-review-bot:v1 markers, but with
    additional qualifiers — they must NOT be treated as original
    findings (reconciling them would create a loop where the bot
    resolves its own conversation replies).
    """
    if INLINE_MARKER_PREFIX not in body:
        return False
    if has_followup_marker(body) or has_reconcile_marker(body):
        return False
    return True


def followup_marker_for(finding_id: str) -> str:
    """Marker for a follow-up reply tied to a specific original finding.

    The id is sanitized — model-provided ids can't break the marker
    syntax even if they contain `-->`, newlines, or arbitrary HTML.
    Loop prevention: future review-comment webhooks check for any
    `FOLLOWUP_MARKER_PREFIX` substring before re-firing.
    """
    return (
        f"<!-- pr-review-bot:v1 followup "
        f"id={_sanitize_finding_id(finding_id)} -->"
    )
