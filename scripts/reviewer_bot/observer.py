"""Observability helper for PR Review Bot runs.

Collects metrics across phases and emits two outputs:
  - Live stdout lines per phase / per tool call (visible in workflow log)
  - Structured markdown table at run end (written to $GITHUB_STEP_SUMMARY,
    visible as a formatted panel in the Actions UI)

Best-effort throughout. Logging failures never break the run.
"""
from __future__ import annotations

import os
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class _TurnRecord:
    turn: int
    finish_reason: str
    tool_calls: list[str] = field(default_factory=list)  # display strings
    prompt_tokens: int = 0
    completion_tokens: int = 0


@dataclass
class Observer:
    """Collects metrics through the run. Methods are best-effort; they
    print to stdout immediately AND accumulate state for the final
    summary table."""

    pr_number: Optional[int] = None
    head_sha: str = ""
    trigger_event: str = ""
    diff_bytes: int = 0
    repo_rules_bytes: int = 0
    touched_specs_tests_bytes: int = 0
    driver_cloned: bool = False
    driver_file_count: int = 0
    driver_listing_bytes: int = 0
    turns: list[_TurnRecord] = field(default_factory=list)
    finding_counts_by_severity: Counter = field(default_factory=Counter)
    inline_count: int = 0
    summary_count: int = 0
    deleted_prior_inline: int = 0
    posted_inline: int = 0
    failed_inline: int = 0
    # v2 posting: "post" | "post (failed)" | "skipped (dry_run)" — names
    # the action taken by the single `POST /pulls/{n}/reviews` call.
    # Renamed from v1's `sticky_summary_action` — there's no sticky any
    # more under v2.
    review_action: str = ""
    failure_reason: Optional[str] = None
    skip_reason: Optional[str] = None  # set when the run exits early without posting
    started_at: float = field(default_factory=time.monotonic)

    def log(self, msg: str) -> None:
        """Print to stdout immediately. Visible in the workflow step log."""
        try:
            print(f"[bot] {msg}", flush=True)
        except Exception:
            pass  # never break the run on a logging error

    def record_phase_2(
        self,
        *,
        diff_bytes: int,
        repo_rules_bytes: int,
        touched_specs_tests_bytes: int,
        driver_cloned: bool,
        driver_file_count: int,
        driver_listing_bytes: int,
    ) -> None:
        self.diff_bytes = diff_bytes
        self.repo_rules_bytes = repo_rules_bytes
        self.touched_specs_tests_bytes = touched_specs_tests_bytes
        self.driver_cloned = driver_cloned
        self.driver_file_count = driver_file_count
        self.driver_listing_bytes = driver_listing_bytes
        self.log(
            f"Phase 2 (gather): diff={diff_bytes:,}B  "
            f"repo_rules={repo_rules_bytes:,}B  "
            f"touched_specs_tests={touched_specs_tests_bytes:,}B  "
            f"driver_cloned={driver_cloned}"
            + (f" (files={driver_file_count:,})" if driver_cloned else "")
        )

    def record_turn(
        self,
        *,
        turn: int,
        finish_reason: str,
        tool_calls: list[str],
        prompt_tokens: int,
        completion_tokens: int,
    ) -> None:
        rec = _TurnRecord(
            turn=turn,
            finish_reason=finish_reason,
            tool_calls=tool_calls,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
        self.turns.append(rec)
        tool_summary = ", ".join(tool_calls) if tool_calls else "(none)"
        self.log(
            f"Phase 3 turn {turn}: finish={finish_reason}  "
            f"tokens={prompt_tokens:,}/{completion_tokens:,}  "
            f"tools=[{tool_summary}]"
        )

    def record_validation(
        self,
        *,
        findings: list[dict[str, Any]],
        inline_eligible_severities: frozenset[str],
    ) -> None:
        """Record findings AFTER validation. `inline_eligible_severities`
        is the set of severity strings that are allowed inline (e.g.,
        {"critical","high","medium"}); Lows/Nits with diff-anchored
        positions still get routed to summary at posting time, so we
        filter here to match what's actually posted.
        """
        for f in findings:
            self.finding_counts_by_severity[f.get("severity", "unknown")] += 1
            inline_routed = f.get("_route") == "inline"
            severity_allows_inline = (
                f.get("severity") in inline_eligible_severities
            )
            if inline_routed and severity_allows_inline:
                self.inline_count += 1
            else:
                self.summary_count += 1
        verdict_parts = []
        for sev in ("critical", "high", "medium", "low", "nit"):
            n = self.finding_counts_by_severity.get(sev, 0)
            if n:
                verdict_parts.append(f"{n} {sev.title()}")
        verdict = " · ".join(verdict_parts) if verdict_parts else "0 findings"
        self.log(
            f"Phase 4 (validate): {verdict}  "
            f"inline={self.inline_count}  summary={self.summary_count}"
        )

    def record_posting(
        self,
        *,
        deleted_prior_inline: int,
        posted_inline: int,
        failed_inline: int,
        review_action: str,
    ) -> None:
        self.deleted_prior_inline = deleted_prior_inline
        self.posted_inline = posted_inline
        self.failed_inline = failed_inline
        self.review_action = review_action
        self.log(
            f"Phase 5 (post): deleted_prior={deleted_prior_inline}  "
            f"posted_inline={posted_inline}  "
            f"failed_inline={failed_inline}  "
            f"review_action={review_action}"
        )

    def record_failure(self, reason: str) -> None:
        self.failure_reason = reason
        self.log(f"FAILURE: {reason[:200]}")

    def record_skip(self, reason: str) -> None:
        """Record that the run is exiting early without posting (e.g.,
        stale head SHA). Surfaced in the step summary so it's obvious
        WHY the run produced no review."""
        self.skip_reason = reason
        self.log(f"SKIP: {reason[:200]}")

    def write_step_summary(self) -> None:
        """Write the final markdown table to $GITHUB_STEP_SUMMARY.
        Best-effort: any I/O error is swallowed silently."""
        path = os.environ.get("GITHUB_STEP_SUMMARY")
        if not path:
            return
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(self._render_summary())
        except OSError:
            pass

    def _render_summary(self) -> str:
        elapsed = time.monotonic() - self.started_at
        lines = ["## PR Review Bot Run", ""]

        # Run metadata
        lines.append("| | |")
        lines.append("|---|---|")
        lines.append(f"| **Trigger** | `{self.trigger_event}` |")
        lines.append(f"| **PR** | #{self.pr_number} |")
        lines.append(f"| **Head SHA** | `{self.head_sha[:12]}` |")
        lines.append(f"| **Elapsed** | {elapsed:.1f}s |")
        if self.failure_reason:
            lines.append(f"| **Outcome** | ⚠️ **FAILED**: `{self.failure_reason[:120]}` |")
        elif self.skip_reason:
            lines.append(f"| **Outcome** | ⏭️ **Skipped**: `{self.skip_reason[:120]}` |")
        else:
            lines.append("| **Outcome** | ✅ Posted review |")
        lines.append("")

        # Prompt budget
        lines.append("### Prompt budget")
        lines.append("| Section | Bytes |")
        lines.append("|---|---|")
        lines.append(f"| Diff | {self.diff_bytes:,} |")
        lines.append(f"| Repo rules | {self.repo_rules_bytes:,} |")
        lines.append(f"| Touched specs/tests | {self.touched_specs_tests_bytes:,} |")
        lines.append(f"| Driver listing | {self.driver_listing_bytes:,} |")
        if self.driver_cloned:
            lines.append(f"| Driver clone | {self.driver_file_count:,} files |")
        lines.append("")

        # Agent loop
        if self.turns:
            lines.append("### Agent loop")
            lines.append("| Turn | finish_reason | Tool calls | Tokens in / out |")
            lines.append("|---|---|---|---|")
            for t in self.turns:
                tools = ", ".join(t.tool_calls) if t.tool_calls else "(none)"
                lines.append(
                    f"| {t.turn} | `{t.finish_reason}` | {tools} | "
                    f"{t.prompt_tokens:,} / {t.completion_tokens:,} |"
                )
            total_prompt = sum(t.prompt_tokens for t in self.turns)
            total_completion = sum(t.completion_tokens for t in self.turns)
            lines.append(
                f"| **Total** | | | **{total_prompt:,} / {total_completion:,}** |"
            )
            lines.append("")

        # Findings
        if self.finding_counts_by_severity:
            lines.append("### Findings")
            lines.append("| Severity | Count | Inline | Summary-only |")
            lines.append("|---|---|---|---|")
            for sev in ("critical", "high", "medium", "low", "nit"):
                n = self.finding_counts_by_severity.get(sev, 0)
                if n == 0:
                    continue
                # We don't track inline/summary per-severity; just totals
                lines.append(f"| {sev.title()} | {n} | — | — |")
            lines.append(
                f"| **Total** | "
                f"**{sum(self.finding_counts_by_severity.values())}** | "
                f"**{self.inline_count}** | **{self.summary_count}** |"
            )
            lines.append("")

        # Posting
        if self.review_action:
            lines.append("### Posting")
            lines.append(f"- Deleted {self.deleted_prior_inline} prior inline comment(s)")
            lines.append(f"- Posted {self.posted_inline} new inline comment(s)")
            if self.failed_inline:
                lines.append(f"- ⚠️ {self.failed_inline} inline post(s) failed (carried into summary)")
            lines.append(f"- Review action: **{self.review_action}**")
            lines.append("")

        lines.append("---")
        lines.append("")
        return "\n".join(lines)
