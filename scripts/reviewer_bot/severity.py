"""Severity scheme and posting thresholds.

Lives in its own module so the constants are immutable, testable, and
trivially imported by every other phase.
"""
from __future__ import annotations

from enum import Enum


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NIT = "nit"


BADGE: dict[Severity, str] = {
    Severity.CRITICAL: "🔴 Critical",
    Severity.HIGH: "🟠 High",
    Severity.MEDIUM: "🟡 Medium",
    Severity.LOW: "🔵 Low",
    Severity.NIT: "⚪ Nit",
}


# Severities eligible for inline comments.
#
# Low findings ARE substantive enough to anchor inline — a file:line
# citation is more useful than burying them in the review-body bullet
# list. Nit (preference / formatting) still routes to the review body
# because an anchor doesn't add value for stylistic-only findings.
INLINE_ELIGIBLE: frozenset[Severity] = frozenset(
    {Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW}
)


def demote_to_low(_: Severity) -> Severity:
    """Used by Phase 4 when a finding fails the verified-against or
    citation check. We don't drop the finding — we keep the signal but
    route it to summary-only."""
    return Severity.LOW
