from scripts.reviewer_bot.markers import (
    SUMMARY_MARKER, INLINE_MARKER_PREFIX, has_summary_marker,
    has_inline_marker, inline_marker_for,
    FOLLOWUP_MARKER_PREFIX, RECONCILE_MARKER,
    has_followup_marker, has_reconcile_marker,
    is_bot_original_finding, followup_marker_for,
)


def test_summary_marker_is_html_comment():
    assert SUMMARY_MARKER.startswith("<!--")
    assert SUMMARY_MARKER.endswith("-->")
    assert "pr-review-bot:v1" in SUMMARY_MARKER
    assert "type=summary" in SUMMARY_MARKER


def test_inline_marker_for_finding_id():
    m = inline_marker_for("F1")
    assert "pr-review-bot:v1" in m
    assert "type=inline" in m
    assert "id=F1" in m


def test_has_summary_marker_detects():
    body = "Some text\n\n<!-- pr-review-bot:v1 type=summary -->"
    assert has_summary_marker(body) is True
    assert has_summary_marker("some other body") is False


def test_has_inline_marker_detects():
    body = "🔴 Critical — fix this\n<!-- pr-review-bot:v1 type=inline id=F3 -->"
    assert has_inline_marker(body) is True
    # Same marker, different id, still counts as an inline marker
    body2 = "<!-- pr-review-bot:v1 type=inline id=F99 -->"
    assert has_inline_marker(body2) is True
    # Summary marker must NOT match inline
    body3 = "<!-- pr-review-bot:v1 type=summary -->"
    assert has_inline_marker(body3) is False


def test_inline_marker_for_strips_html_comment_terminator():
    """A malicious id containing `-->` must not be able to close the
    HTML comment early. Sanitizer strips disallowed characters."""
    m = inline_marker_for("F1 --> <script>alert(1)</script>")
    # Marker must still be a single, well-formed HTML comment.
    assert m.startswith("<!--")
    assert m.endswith("-->")
    # Only one `-->` (the closer) — none from the injected payload.
    assert m.count("-->") == 1
    assert "<script>" not in m


def test_inline_marker_for_strips_newlines():
    """Newlines in the id would split the HTML comment across lines and
    confuse Markdown rendering / our own marker detection."""
    m = inline_marker_for("F1\nid=injected")
    assert "\n" not in m
    assert "\r" not in m
    # Sanitization preserves only allowed chars: F1, id, injected glued together
    assert m == "<!-- pr-review-bot:v1 type=inline id=F1idinjected -->"


def test_inline_marker_for_caps_length():
    """A pathologically long id is capped so the comment stays bounded."""
    long_id = "F" + "x" * 500
    m = inline_marker_for(long_id)
    # Total marker length is bounded; the id portion never exceeds 40 chars.
    # Marker fixed prefix + suffix is ~37 chars; total stays < ~80.
    assert len(m) < 100
    assert m.startswith("<!-- pr-review-bot:v1 type=inline id=")
    assert m.endswith(" -->")


def test_inline_marker_for_empty_id_falls_back():
    m = inline_marker_for("")
    assert "id=unknown" in m


def test_inline_marker_for_none_id_falls_back():
    m = inline_marker_for(None)  # type: ignore[arg-type]
    assert "id=unknown" in m


# ─── Phase 2 markers (followup + reconcile) ────────────────────────────


def test_reconcile_marker_is_html_comment():
    assert RECONCILE_MARKER.startswith("<!--")
    assert RECONCILE_MARKER.endswith("-->")
    assert "pr-review-bot:v1" in RECONCILE_MARKER
    assert "reconcile" in RECONCILE_MARKER


def test_followup_marker_for_finding_id():
    m = followup_marker_for("F1")
    assert "pr-review-bot:v1" in m
    assert "followup" in m
    assert "id=F1" in m


def test_has_followup_marker_detects():
    body = "Reply text\n<!-- pr-review-bot:v1 followup id=F1 -->"
    assert has_followup_marker(body)
    assert not has_followup_marker("plain text")
    # Reconcile marker must NOT match followup
    assert not has_followup_marker(
        "<!-- pr-review-bot:v1 reconcile -->"
    )


def test_has_reconcile_marker_detects():
    body = "Resolved.\n<!-- pr-review-bot:v1 reconcile -->"
    assert has_reconcile_marker(body)
    assert not has_reconcile_marker("plain text")
    assert not has_reconcile_marker(
        "<!-- pr-review-bot:v1 followup id=F1 -->"
    )


def test_is_bot_original_finding_true_for_inline_marker():
    body = "🔴 Critical — fix\n<!-- pr-review-bot:v1 type=inline id=F1 -->"
    assert is_bot_original_finding(body)


def test_is_bot_original_finding_false_for_followup():
    """A body containing the followup marker must NOT count as an
    original finding — otherwise reconcile would loop on its own
    replies. Even when the followup marker also (incidentally) carries
    the broader v1 prefix, the more-specific qualifier wins."""
    body = (
        "Thanks.\n"
        "<!-- pr-review-bot:v1 followup id=F1 -->"
    )
    assert not is_bot_original_finding(body)


def test_is_bot_original_finding_false_for_reconcile():
    body = "Resolved.\n<!-- pr-review-bot:v1 reconcile -->"
    assert not is_bot_original_finding(body)


def test_is_bot_original_finding_false_when_no_marker():
    """Human / other-bot comments without our v1 inline marker must NOT
    be treated as bot original findings."""
    assert not is_bot_original_finding("hey nice work, but…")
    # Summary marker is also not an inline finding.
    assert not is_bot_original_finding(
        "<!-- pr-review-bot:v1 type=summary -->"
    )


def test_followup_marker_for_sanitizes_id():
    """Same sanitization invariants as inline_marker_for — a malicious
    id can't break the HTML comment."""
    m = followup_marker_for("F1 --> <script>alert(1)</script>")
    assert m.startswith("<!--")
    assert m.endswith("-->")
    assert m.count("-->") == 1
    assert "<script>" not in m
    assert "\n" not in m
