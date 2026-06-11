"""Tests for shared.markers — generic namespaced HTML marker builders."""
from __future__ import annotations

from scripts.shared import markers


class TestMarkerBuilder:
    def test_simple_marker_for_namespace(self):
        m = markers.marker("engineer-bot", "fix-push")
        assert m == "<!-- engineer-bot:v1 fix-push -->"

    def test_marker_with_id_sanitization(self):
        # Sanitization strips `>` (and any other char outside [A-Za-z0-9_.-])
        # but KEEPS `-` so ids like F1-followup survive. The security
        # invariant is that the marker contains exactly one `-->` (the
        # closer), which is guaranteed by stripping `>`.
        m = markers.marker_with_id("engineer-bot", "followup", "F1-->evil")
        assert m.count("-->") == 1
        assert m.endswith("-->")
        # No injected HTML in the id portion (the marker itself starts
        # with `<!--`, so we check just the id field).
        assert "<" not in m.split("id=", 1)[1]
        # `>` stripped, `-` kept twice → "F1--evil"
        assert "id=F1--evil" in m

    def test_marker_with_id_caps_length(self):
        long_id = "x" * 200
        m = markers.marker_with_id("engineer-bot", "followup", long_id)
        assert "id=" + ("x" * 40) + " -->" in m

    def test_marker_with_id_empty_falls_back(self):
        m = markers.marker_with_id("ns", "k", "")
        assert "id=unknown" in m

    def test_marker_with_id_none_falls_back(self):
        m = markers.marker_with_id("ns", "k", None)
        assert "id=unknown" in m

    def test_has_marker_substring_match(self):
        body = "Some text\n<!-- engineer-bot:v1 fix-push -->\nMore"
        assert markers.has_marker(body, "engineer-bot", "fix-push")
        assert not markers.has_marker(body, "engineer-bot", "review")
        assert not markers.has_marker(body, "other-bot", "fix-push")

    def test_has_any_marker_for_namespace(self):
        body = "<!-- engineer-bot:v1 anything -->"
        assert markers.has_any_marker(body, "engineer-bot")
        assert not markers.has_any_marker(body, "reviewer-bot")

    def test_has_any_marker_skips_blockquoted_line(self):
        """When a reviewer quotes the bot's prior reply via GitHub's
        blockquote syntax (`>` prefix), the quoted body — including
        the marker — appears in the reviewer's reply. has_any_marker
        must NOT match those quoted markers, otherwise the reviewer's
        reply is misclassified as bot-authored and suppresses
        re-engagement / breaks the own-reply loop guards."""
        body = (
            "Reviewer's reply:\n"
            "> previous bot text\n"
            "> <!-- engineer-bot:v1 followup -->\n"
        )
        assert markers.has_any_marker(body, "engineer-bot") is False

    def test_has_any_marker_matches_indented_quote_too(self):
        """Markdown allows leading whitespace before `>` in nested
        quotes. The skip must apply regardless of indent."""
        body = "Outer:\n  > <!-- engineer-bot:v1 inner -->\n"
        assert markers.has_any_marker(body, "engineer-bot") is False

    def test_has_any_marker_finds_marker_alongside_quote(self):
        """If a body has BOTH a quoted (irrelevant) marker AND an
        unquoted (active) marker, the active one wins."""
        body = (
            "> <!-- engineer-bot:v1 quoted -->\n"
            "<!-- engineer-bot:v1 active -->\n"
        )
        assert markers.has_any_marker(body, "engineer-bot") is True

    def test_has_marker_skips_blockquoted_line(self):
        """Same blockquote rule applies to `has_marker` (specific
        kind), since the same false-positive surface exists."""
        body = "> <!-- engineer-bot:v1 fix-push -->\n"
        assert markers.has_marker(body, "engineer-bot", "fix-push") is False
