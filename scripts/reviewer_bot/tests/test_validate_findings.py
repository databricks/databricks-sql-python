from scripts.reviewer_bot.validate_findings import (
    MAX_SUMMARY_CHARS,
    demote_uncited_or_unverified,
    route_by_diff_position,
    truncate_summary,
)


def test_finding_inside_diff_routes_inline():
    finding = {"id": "F1", "severity": "high", "file": "src/foo.py", "line": 11}
    diff_positions = {"src/foo.py": {10, 11, 12}}

    routed = route_by_diff_position([finding], diff_positions)
    assert routed[0]["_route"] == "inline"


def test_finding_outside_diff_routes_summary():
    finding = {"id": "F1", "severity": "high", "file": "src/foo.py", "line": 50}
    diff_positions = {"src/foo.py": {10, 11, 12}}

    routed = route_by_diff_position([finding], diff_positions)
    assert routed[0]["_route"] == "summary"


def test_finding_with_unknown_file_routes_summary():
    finding = {"id": "F1", "severity": "high", "file": "src/other.py", "line": 5}
    diff_positions = {"src/foo.py": {10, 11, 12}}

    routed = route_by_diff_position([finding], diff_positions)
    assert routed[0]["_route"] == "summary"


def test_finding_without_file_or_line_routes_summary():
    finding = {"id": "F1", "severity": "low"}
    diff_positions = {"src/foo.py": {10}}

    routed = route_by_diff_position([finding], diff_positions)
    assert routed[0]["_route"] == "summary"


def test_route_does_not_mutate_input():
    finding = {"id": "F1", "severity": "high", "file": "src/foo.py", "line": 11}
    diff_positions = {"src/foo.py": {11}}

    route_by_diff_position([finding], diff_positions)
    assert "_route" not in finding  # original untouched


def test_finding_with_citation_kept():
    f = {"id": "F1", "severity": "high", "citation": "CLAUDE.md:42 — rule X",
         "body": "regular finding"}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "high"


def test_finding_without_citation_demoted_to_nit():
    """Uncited finding demotes to `nit`, not `low`. Under the v2 posting
    model `low` is INLINE_ELIGIBLE — demoting to `low` would let an
    uncited high finding still land inline on a diff line, defeating
    the demotion's purpose. `nit` is the only non-inline-eligible
    severity, so demoting there keeps it in the summary."""
    f = {"id": "F1", "severity": "high", "body": "no rule cited"}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit"


def test_absence_claim_without_verified_against_demoted():
    """Absence-claim without verified_against also demotes to `nit`
    (see test_finding_without_citation_demoted_to_nit for rationale)."""
    f = {
        "id": "F1",
        "severity": "high",
        "citation": "CLAUDE.md:42",
        "body": "There is no validation on this input.",
        # missing verified_against
    }
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit"


def test_absence_claim_with_verified_against_kept():
    f = {
        "id": "F1",
        "severity": "high",
        "citation": "CLAUDE.md:42",
        "verified_against": "src/foo.py:30-55",
        "body": "There is no validation on this input.",
    }
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "high"


def test_non_absence_claim_doesnt_need_verified_against():
    f = {
        "id": "F1",
        "severity": "high",
        "citation": "CLAUDE.md:42",
        "body": "This name doesn't match the convention.",
    }
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "high"


from scripts.reviewer_bot.validate_findings import dedupe_findings


def test_dedup_keeps_higher_severity():
    findings = [
        {"id": "F1", "severity": "medium", "file": "a.py", "line": 5,
         "body": "Same issue described one way."},
        {"id": "F2", "severity": "high", "file": "a.py", "line": 5,
         "body": "Same issue described another way."},
    ]
    out = dedupe_findings(findings)
    assert len(out) == 1
    assert out[0]["severity"] == "high"


def test_dedup_keeps_distinct_lines():
    findings = [
        {"id": "F1", "severity": "high", "file": "a.py", "line": 5, "body": "x"},
        {"id": "F2", "severity": "high", "file": "a.py", "line": 6, "body": "y"},
    ]
    out = dedupe_findings(findings)
    assert len(out) == 2


def test_dedup_keeps_distinct_files():
    findings = [
        {"id": "F1", "severity": "high", "file": "a.py", "line": 5, "body": "x"},
        {"id": "F2", "severity": "high", "file": "b.py", "line": 5, "body": "x"},
    ]
    out = dedupe_findings(findings)
    assert len(out) == 2


def test_dedup_keeps_findings_without_location():
    findings = [
        {"id": "F1", "severity": "low", "body": "no location 1"},
        {"id": "F2", "severity": "low", "body": "no location 2"},
    ]
    out = dedupe_findings(findings)
    assert len(out) == 2


def test_benign_without_does_not_demote():
    f = {"id": "F1", "severity": "high", "citation": "CLAUDE.md:1",
         "body": "Refactor this method without breaking the API contract."}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "high"


def test_benign_missing_does_not_demote():
    f = {"id": "F1", "severity": "high", "citation": "CLAUDE.md:1",
         "body": "The missing semicolon was already fixed upstream."}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "high"


def test_specific_no_validation_still_demoted():
    f = {"id": "F1", "severity": "high", "citation": "CLAUDE.md:1",
         "body": "There is no validation on the input parameter."}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit"


def test_missing_with_object_still_demoted():
    f = {"id": "F1", "severity": "high", "citation": "CLAUDE.md:1",
         "body": "Missing null check before dereference."}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit"


def test_demote_handles_non_string_body():
    """Model can emit `body: null` (or even non-string types).
    _is_absence_claim must not call re.search on a non-str."""
    # body=None
    f1 = {"id": "F1", "severity": "high", "citation": "CLAUDE.md:1", "body": None}
    out = demote_uncited_or_unverified([f1])
    # Not an absence claim (can't be — body isn't a string) → not demoted on
    # the absence path. Has citation, so not demoted on the citation path
    # either. Severity stays "high".
    assert out[0]["severity"] == "high"

    # body missing entirely
    f2 = {"id": "F2", "severity": "high", "citation": "CLAUDE.md:1"}
    out = demote_uncited_or_unverified([f2])
    assert out[0]["severity"] == "high"

    # body is an int (pathological)
    f3 = {"id": "F3", "severity": "high", "citation": "CLAUDE.md:1", "body": 42}
    out = demote_uncited_or_unverified([f3])
    assert out[0]["severity"] == "high"


def test_demote_does_not_re_demote_already_nit():
    """A 'nit' finding without a citation must STAY 'nit'. Earlier code
    unconditionally set severity='low' for uncited findings — which
    actually PROMOTED a nit (nit < low) rather than demoted it. Under
    the v2 behavior (demote to `nit`), an already-nit finding is a
    no-op."""
    f = {"id": "F1", "severity": "nit", "body": "no citation"}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit", (
        "nit must not be promoted — function is named DEMOTE"
    )


def test_demote_low_uncited_now_demoted_to_nit():
    """Under the v2 demote-to-nit behavior, an uncited Low finding gets
    demoted to Nit. The prior implementation (demote-to-low) left Low
    alone, but with Low now INLINE_ELIGIBLE that meant an uncited Low
    on a diff line still landed inline — defeating the demotion's
    purpose. Demoting to Nit ensures uncited findings stay in summary
    regardless of diff anchorability."""
    f = {"id": "F1", "severity": "low", "body": "no citation"}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit"


def test_demote_nit_absence_claim_stays_nit():
    """Absence-claim path also must not promote nit."""
    f = {
        "id": "F1",
        "severity": "nit",
        "citation": "CLAUDE.md:1",
        # absence claim but no verified_against
        "body": "There is no validation on this input.",
    }
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "nit"


def test_demote_unknown_severity_uncited_treated_as_nit_floor():
    """An uncited finding with an unrecognized severity is treated as if
    its rank were the nit floor → not changed (since nit == nit). This
    prevents the function from accidentally normalizing unrecognized
    severities into "nit" silently — the orchestrator already drops
    findings with invalid severity at posting time."""
    f = {"id": "F1", "severity": "unknown", "body": "no citation"}
    out = demote_uncited_or_unverified([f])
    assert out[0]["severity"] == "unknown"


# ── truncate_summary (MAX_SUMMARY_CHARS defense) ──────────────────


def test_truncate_summary_passes_short_text_unchanged():
    """Short summaries (under the cap) come back verbatim, no suffix."""
    s = "Looks good — 1 medium concern around X."
    assert truncate_summary(s) == s


def test_truncate_summary_at_exact_cap_unchanged():
    """A summary exactly at the cap is NOT truncated."""
    s = "a" * MAX_SUMMARY_CHARS
    assert truncate_summary(s) == s
    assert "truncated" not in truncate_summary(s)


def test_truncate_summary_long_text_truncated_with_suffix():
    """Over-cap summary is truncated AND the suffix is appended so the
    reviewer can tell something was cut. The FINAL string (prefix +
    suffix) honors MAX_SUMMARY_CHARS — the suffix is counted INSIDE the
    cap, not appended on top of it (earlier behavior let the result
    exceed the cap, contradicting the docstring)."""
    s = "x" * (MAX_SUMMARY_CHARS + 500)
    out = truncate_summary(s)
    # Suffix is appended visibly so reviewers see the indicator.
    assert "truncated to keep verdict scannable" in out
    # Prefix is preserved up to (cap - suffix_len) — leaving room for
    # the suffix to bring the total exactly to the cap.
    assert out.endswith("[...truncated to keep verdict scannable]")
    # Final length honors the cap (suffix counted INSIDE the cap).
    assert len(out) == MAX_SUMMARY_CHARS


def test_truncate_summary_handles_non_string():
    """Defensive: non-string inputs (None, int, dict) coerce to '' —
    matches the orchestrator's existing `result.get('summary') or ''`
    pattern. We must not crash on weird model output here."""
    assert truncate_summary(None) == ""
    assert truncate_summary(42) == ""
    assert truncate_summary({"weird": "shape"}) == ""


def test_dedupe_returns_independent_copies():
    """The pipeline contract is that validators return new lists of
    new dicts — downstream mutation must not leak back into the input."""
    findings = [
        {"id": "F1", "severity": "high", "file": "a.py", "line": 5,
         "body": "original"},
        {"id": "F2", "severity": "low", "body": "no location"},
    ]
    out = dedupe_findings(findings)

    # Mutate the dedupe output
    for f in out:
        f["body"] = "MUTATED"
        f["_route"] = "inline"

    # Original findings untouched
    assert findings[0]["body"] == "original"
    assert findings[1]["body"] == "no location"
    assert "_route" not in findings[0]
    assert "_route" not in findings[1]
