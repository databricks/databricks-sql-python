"""Tests for the Observer (observability helper)."""
import os

import pytest

from scripts.reviewer_bot.observer import Observer


def test_observer_records_phases_and_renders_summary(tmp_path, monkeypatch):
    """End-to-end: record everything, write summary to a temp GITHUB_STEP_SUMMARY."""
    summary_path = tmp_path / "step_summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_path))

    obs = Observer(
        pr_number=42,
        head_sha="abc123def456789",
        trigger_event="pull_request",
    )
    obs.record_phase_2(
        diff_bytes=1234,
        repo_rules_bytes=5678,
        touched_specs_tests_bytes=910,
        driver_cloned=True,
        driver_file_count=100,
        driver_listing_bytes=2000,
    )
    obs.record_turn(
        turn=1,
        finish_reason="tool_calls",
        tool_calls=["grep('CreateProxiedConnection')"],
        prompt_tokens=5000,
        completion_tokens=150,
    )
    obs.record_turn(
        turn=2,
        finish_reason="tool_calls",
        tool_calls=["finalize_review(3 findings)"],
        prompt_tokens=6000,
        completion_tokens=1200,
    )
    obs.record_validation(
        findings=[
            {"severity": "medium", "_route": "inline"},
            {"severity": "low", "_route": "summary"},
            {"severity": "low", "_route": "summary"},
        ],
        inline_eligible_severities=frozenset({"critical", "high", "medium"}),
    )
    obs.record_posting(
        deleted_prior_inline=2,
        posted_inline=1,
        failed_inline=0,
        review_action="post",
    )
    obs.write_step_summary()

    text = summary_path.read_text()
    assert "## PR Review Bot Run" in text
    assert "#42" in text
    assert "abc123def456" in text
    assert "pull_request" in text
    assert "1,234" in text  # diff_bytes formatted
    assert "Turn" in text  # agent loop table header
    assert "tool_calls" in text  # finish_reason value
    assert "5,000 / 150" in text  # tokens
    assert "**11,000 / 1,350**" in text  # tokens total (bolded): 5,000 + 6,000 in, 150 + 1,200 out
    assert "Medium" in text
    assert "Low" in text
    assert "Deleted 2 prior" in text
    # v2 field name: review_action (was sticky_summary_action under v1).
    assert "Review action" in text
    assert "**post**" in text


def test_observer_without_github_step_summary_silent(monkeypatch):
    """If GITHUB_STEP_SUMMARY is not set, write_step_summary is a no-op."""
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    obs = Observer(pr_number=1, head_sha="x", trigger_event="t")
    obs.write_step_summary()  # must not raise


def test_observer_failure_reflected_in_summary(tmp_path, monkeypatch):
    summary_path = tmp_path / "step_summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_path))

    obs = Observer(pr_number=99, head_sha="def", trigger_event="x")
    obs.record_failure("LLM endpoint timeout after 3 attempts")
    obs.write_step_summary()

    text = summary_path.read_text()
    assert "FAILED" in text
    assert "LLM endpoint timeout" in text


def test_observer_inline_count_excludes_low_severity_with_diff_position():
    """Regression: a Low finding with `_route=inline` (because it
    happened to have a diff-anchored file:line) must NOT count as inline
    in the observer — Phase 5's INLINE_ELIGIBLE filter will route it to
    summary at posting time."""
    obs = Observer(pr_number=1, head_sha="x", trigger_event="t")
    obs.record_validation(
        findings=[
            {"severity": "high", "_route": "inline"},      # → inline
            {"severity": "medium", "_route": "inline"},    # → inline
            {"severity": "low", "_route": "inline"},       # → summary (severity-filtered)
            {"severity": "nit", "_route": "inline"},       # → summary (severity-filtered)
            {"severity": "low", "_route": "summary"},      # → summary
        ],
        inline_eligible_severities=frozenset({"critical", "high", "medium"}),
    )
    assert obs.inline_count == 2  # only High + Medium with inline route
    assert obs.summary_count == 3  # 2 Lows + 1 Nit


def test_observer_log_does_not_raise_on_print_failure(monkeypatch):
    """log() is best-effort; even if print fails, observer keeps working."""
    def boom(*args, **kwargs):
        raise IOError("stdout closed")
    monkeypatch.setattr("builtins.print", boom)
    obs = Observer(pr_number=1, head_sha="x", trigger_event="t")
    obs.log("test message")  # must not raise
