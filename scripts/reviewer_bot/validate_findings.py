"""Phase 4: validation pass over parsed findings before posting.

All functions here are pure and return a NEW list of findings with
mutations applied — they never modify the input findings in place.
This makes the pipeline easy to debug (each stage's output can be
inspected) and easy to test.
"""
from __future__ import annotations

import copy
import re
from typing import Any


def route_by_diff_position(
    findings: list[dict[str, Any]],
    diff_positions: dict[str, set[int]],
) -> list[dict[str, Any]]:
    """Set `_route` on each finding to `inline` or `summary` based on
    whether its file:line lands on an added or modified RIGHT-side line
    (i.e. a `+` line in the diff). `diff_positions` only tracks those —
    not context (` `) lines or removed (`-`) lines — because GitHub's
    inline-comment API only accepts positions on RIGHT-side changes.
    Findings without a file or line always route to summary.
    """
    out: list[dict[str, Any]] = []
    for f in findings:
        new_f = copy.deepcopy(f)
        file_ = new_f.get("file")
        line = new_f.get("line")
        if (
            file_
            and isinstance(line, int)
            and line in diff_positions.get(file_, set())
        ):
            new_f["_route"] = "inline"
        else:
            new_f["_route"] = "summary"
        out.append(new_f)
    return out


# Absence-claim patterns used to require `verified_against`. Multi-word
# phrases (with `\b` boundaries) so that benign text like "refactor without
# breaking the API" or "missing semicolon fixed" doesn't trigger.
_ABSENCE_PATTERNS = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bno validation\b",
        r"\bno (?:null|nil) (?:check|guard)\b",
        r"\bno error handling\b",
        r"\bno (?:cleanup|teardown)\b",
        r"\bnot present\b",
        r"\bmissing (?:validation|null check|null guard|error handling|cleanup|teardown|guard)\b",
        r"\b(?:doesn't|does not) (?:validate|handle|check|null[- ]check|guard)\b",
        r"\b(?:is|are|seems?) (?:absent|missing)\b",
        r"\blacks (?:validation|error handling|null check|null guard|cleanup|teardown)\b",
        r"\bwithout (?:validation|null check|error handling|cleanup|teardown|guard)\b",
        r"\bno check (?:for|on)\b",
    )
)


def _is_absence_claim(body: object) -> bool:
    # Model output can be anything; `body` may be None, an int, or even
    # a dict. `re.Pattern.search` raises TypeError on non-str — guard
    # before iterating.
    if not isinstance(body, str):
        return False
    return any(p.search(body) for p in _ABSENCE_PATTERNS)


_SEVERITY_RANK = {
    "critical": 4, "high": 3, "medium": 2, "low": 1, "nit": 0,
}


def demote_uncited_or_unverified(
    findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Demote to severity `nit` any finding that:
      - has no `citation` field, OR
      - is an absence claim (body matches an absence phrase) but has
        no `verified_against`.
    Demoting preserves the finding (still posted in summary) but
    prevents low-confidence noise from getting inline placement.

    Why `nit` and not `low`? Under the v2 posting model `low` is
    INLINE_ELIGIBLE — so demoting an uncited High → Low would still
    let it land as an inline comment on a diff line, defeating the
    point of demoting (which is "we don't fully trust this; bury it
    in the summary so reviewers can skim past easily"). `nit` is the
    only non-inline-eligible severity, so demoting there keeps the
    finding visible in the summary's "Other findings" section
    regardless of diff anchorability. The label is imperfect — a
    demoted absence-claim isn't strictly a "preference/formatting"
    nit — but the inline-suppression property is what matters; the
    body text the model wrote stays accurate either way.

    Only applies when the *current* severity ranks ABOVE nit. A finding
    already at nit is left alone — otherwise this function would be a
    no-op there (a finding already at nit is already non-inline).
    """
    out: list[dict[str, Any]] = []
    nit_rank = _SEVERITY_RANK["nit"]
    for f in findings:
        new_f = copy.deepcopy(f)
        body = new_f.get("body", "")
        has_citation = bool(new_f.get("citation"))
        has_verified = bool(new_f.get("verified_against"))
        absence = _is_absence_claim(body)

        if not has_citation or (absence and not has_verified):
            # Only set to "nit" if the current severity is STRICTLY
            # higher than nit. Leaves nit untouched. Unrecognized
            # severities default to nit_rank → also untouched, which
            # preserves the original "don't silently normalize unknown
            # severities" property of the previous implementation.
            current_rank = _SEVERITY_RANK.get(new_f.get("severity"), nit_rank)
            if current_rank > nit_rank:
                new_f["severity"] = "nit"
        out.append(new_f)
    return out


# Defensive cap on the LLM-emitted summary. SYSTEM_PROMPT already
# constrains the model to 2-3 sentences; this is the belt to that
# suspenders so a model that ignores the prompt can't blow up the
# review body. 600 chars ≈ 3 long sentences.
MAX_SUMMARY_CHARS = 600
_SUMMARY_TRUNCATION_SUFFIX = "\n\n[...truncated to keep verdict scannable]"


def truncate_summary(summary: object) -> str:
    """Cap `summary` at MAX_SUMMARY_CHARS (final-string length, INCLUDING
    the truncation-marker suffix), appending the suffix when we cut.
    Non-string inputs coerce to "" (the orchestrator already handles
    missing/None summary).

    Implementation: reserve `len(_SUMMARY_TRUNCATION_SUFFIX)` bytes for
    the suffix BEFORE slicing the input. Earlier behavior sliced to the
    cap and THEN appended the suffix, which let the returned string
    exceed MAX_SUMMARY_CHARS — contradicting the docstring's contract.
    """
    if not isinstance(summary, str):
        return ""
    if len(summary) <= MAX_SUMMARY_CHARS:
        return summary
    prefix_cap = MAX_SUMMARY_CHARS - len(_SUMMARY_TRUNCATION_SUFFIX)
    return summary[:prefix_cap] + _SUMMARY_TRUNCATION_SUFFIX


def dedupe_findings(
    findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """When two findings share the same (file, line), keep only the
    higher-severity one. Findings without a file/line are never
    deduped against each other.
    """
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    no_location: list[dict[str, Any]] = []

    for f in findings:
        # deepcopy so downstream mutation never bleeds back into the
        # input list — matches this module's "pure, returns a new list"
        # contract documented at the top.
        f = copy.deepcopy(f)
        file_ = f.get("file")
        line = f.get("line")
        if not file_ or not isinstance(line, int):
            no_location.append(f)
            continue

        key = (file_, line)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = f
        else:
            old_rank = _SEVERITY_RANK.get(existing.get("severity", "low"), 0)
            new_rank = _SEVERITY_RANK.get(f.get("severity", "low"), 0)
            if new_rank > old_rank:
                by_key[key] = f

    return list(by_key.values()) + no_location
