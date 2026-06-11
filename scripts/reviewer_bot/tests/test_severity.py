import pytest
from scripts.reviewer_bot.severity import (
    Severity, BADGE, INLINE_ELIGIBLE, demote_to_low,
)


def test_severity_values():
    assert Severity.CRITICAL.value == "critical"
    assert Severity.HIGH.value == "high"
    assert Severity.MEDIUM.value == "medium"
    assert Severity.LOW.value == "low"
    assert Severity.NIT.value == "nit"


def test_badges_present_for_every_severity():
    for s in Severity:
        assert s in BADGE
        assert len(BADGE[s]) > 0


def test_inline_eligible_set():
    """v2 contract: Critical/High/Medium/Low are inline-eligible.
    Nit is the only severity still summary-only — a file:line anchor
    doesn't add value for preference / formatting findings."""
    assert Severity.CRITICAL in INLINE_ELIGIBLE
    assert Severity.HIGH in INLINE_ELIGIBLE
    assert Severity.MEDIUM in INLINE_ELIGIBLE
    assert Severity.LOW in INLINE_ELIGIBLE
    assert Severity.NIT not in INLINE_ELIGIBLE


def test_demote_to_low_returns_low():
    assert demote_to_low(Severity.HIGH) == Severity.LOW
    assert demote_to_low(Severity.LOW) == Severity.LOW
