"""Phase 5: post the review.

v2 posting model — one PR review event per push.

Each run issues a single `POST /repos/{owner}/{repo}/pulls/{n}/reviews`
call with `event=COMMENT`, a short body (the LLM's verdict), and a
`comments[]` array bundling every inline-routed finding. This produces
a discrete timeline entry per push instead of the old in-place sticky
patch that reviewers had to notice "edited" on.

The bot does NOT delete prior reviews or their inline comments — they
belong to past timeline events. Phase 6 (reconcile) walks the bot's
OPEN finding threads and resolves the ones whose finding wasn't
re-raised this run; that handles "stale thread" cleanup naturally
without touching the GitHub timeline.

`SUMMARY_MARKER` is intentionally NOT used by new posts. It remains
defined in `markers.py` so a future migration-cleanup pass can
identify v1 stickies left over from the old model.
"""
from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

from .markers import has_inline_marker


BOT_LOGIN = "peco-review-bot[bot]"

# Migration-window: also recognize the legacy login. Before this app
# migration, the bot posted as the shared `github-actions[bot]`
# identity. PRs that were already open when the app migration landed
# may have inline comments authored as the legacy login; without
# dual-login recognition, the new bot's reconcile would skip them →
# stale comments accumulate on those in-flight PRs.
#
# The marker check (has_inline_marker) is the real security gate —
# login is defense-in-depth. After all pre-migration PRs are
# closed/merged, the legacy entry can be removed in a follow-up
# cleanup PR.
BOT_LOGINS = (BOT_LOGIN, "github-actions[bot]")


def _is_bot(comment: dict[str, Any]) -> bool:
    return (comment.get("user") or {}).get("login") in BOT_LOGINS


def pick_bot_inline_comment_ids(
    inline_comments: list[dict[str, Any]],
) -> list[int]:
    """Filter to inline comments authored by the bot AND carrying our
    v1 inline marker.

    "Authored by the bot" matches either the current identity
    (`peco-review-bot[bot]`) OR the legacy shared identity
    (`github-actions[bot]`) during the migration window — see
    BOT_LOGINS for the rationale. The marker check is the real
    security gate; the login filter is defense-in-depth.

    Used by reconcile (Phase 6) to identify the bot's own threads
    when deciding which ones to resolve."""
    return [
        c["id"]
        for c in inline_comments
        if _is_bot(c) and has_inline_marker(c.get("body", ""))
    ]


def cleanup_orphan_pending_reviews(
    *, repo: str, pr_number: int, bot_login: str = BOT_LOGIN,
) -> int:
    """Delete any PENDING review by `bot_login` on this PR.

    Pending reviews are draft reviews created server-side when a
    `POST /pulls/{n}/reviews` call begins processing but fails before
    the review is submitted (e.g. validation failure on the
    `comments[]` array, OR — historically — a misuse of `gh api`
    where field flags `-f` / `-F` flip the implicit method from GET to
    POST. POSTing to `/pulls/{n}/reviews` with no `event` field
    creates an empty PENDING review draft; once one exists, every
    subsequent submit fails until it's deleted).

    GitHub allows only one pending review per author per PR. If an
    orphan is left behind, every subsequent `post_pr_review` call
    422s with `"User can only have one pending review per pull
    request"` until it's deleted — and pending reviews are PRIVATE to
    their author (REST and GraphQL hide them from third parties), so
    only the bot itself (running under its own App-installation
    token) can list and DELETE its own pending drafts.

    Run this at the start of Phase 5 so the upcoming POST is
    guaranteed a clean slate. Idempotent: returns the number of
    pending reviews deleted (0 in the healthy case). Failures of the
    listing call OR the DELETE call are logged as warnings and
    swallowed — they shouldn't block the actual review post; if a
    real orphan remains, the subsequent POST will surface the 422
    and the diagnostic in the failure branch logs the specifics.
    """
    bot_login_lc = bot_login.lower()
    deleted = 0
    page = 1
    # GitHub returns reviews oldest-first; an orphan PENDING from a
    # recent failed POST is the NEWEST entry, which on a high-traffic
    # PR (motivating example: PR #382 with 585 review submissions)
    # lands on the LAST page, not the first. Without explicit
    # pagination this cleanup would silently miss the exact orphans
    # it was written to defend against. We pass `--method GET`
    # explicitly so `-F page=...` doesn't silently flip `gh api` to
    # POST and create a fresh PENDING review of its own.
    # We intentionally do NOT pass `--paginate`:
    # `gh api` defaults to single-page when the flag is absent, which
    # is exactly what we want \u2014 we walk pages manually below. An
    # earlier revision passed `--paginate=false` belt-and-suspenders,
    # but that spelling isn't a documented `gh` flag (only
    # `--paginate` as a bare boolean is); on stricter `gh` versions
    # it errors out and the cleanup silently no-ops, leaving the very
    # orphan PENDING reviews this function was written to delete.
    while True:
        list_result = subprocess.run(
            ["gh", "api", "--method", "GET",
             f"repos/{repo}/pulls/{pr_number}/reviews",
             "-F", "per_page=100",
             "-F", f"page={page}"],
            check=False, capture_output=True, text=True,
        )
        if list_result.returncode != 0:
            print(
                f"::warning::cleanup_orphan_pending_reviews list failed "
                f"pr={pr_number} page={page} rc={list_result.returncode}: "
                f"{(list_result.stderr or '').strip()[:400]}",
                file=sys.stderr,
            )
            return deleted

        try:
            reviews = json.loads(list_result.stdout)
        except (ValueError, TypeError):
            print(
                f"::warning::cleanup_orphan_pending_reviews could not parse "
                f"reviews JSON for pr={pr_number} page={page}",
                file=sys.stderr,
            )
            return deleted

        if not isinstance(reviews, list) or not reviews:
            break

        for review in reviews:
            if not isinstance(review, dict):
                continue
            if (review.get("state") or "").upper() != "PENDING":
                continue
            author = ((review.get("user") or {}).get("login") or "").lower()
            if author != bot_login_lc:
                continue
            review_id = review.get("id")
            if review_id is None:
                continue
            # `gh api -X DELETE` doesn't need a body for this endpoint.
            del_result = subprocess.run(
                ["gh", "api", "--method", "DELETE",
                 f"repos/{repo}/pulls/{pr_number}/reviews/{review_id}"],
                check=False, capture_output=True, text=True,
            )
            if del_result.returncode != 0:
                print(
                    f"::warning::cleanup_orphan_pending_reviews DELETE failed "
                    f"pr={pr_number} review_id={review_id} "
                    f"rc={del_result.returncode}: "
                    f"{(del_result.stderr or '').strip()[:400]}",
                    file=sys.stderr,
                )
                continue
            print(
                f"::notice::cleanup_orphan_pending_reviews deleted orphan "
                f"pending review id={review_id} pr={pr_number}",
                file=sys.stderr,
            )
            deleted += 1

        # Page wasn't full → no more pages.
        if len(reviews) < 100:
            break
        page += 1
        # Hard cap on pagination so a runaway loop on a malformed
        # response can't hang the workflow. 50 pages × 100 per page
        # = 5,000 reviews; well beyond any real PR.
        if page > 50:
            break
    return deleted


def post_pr_review(
    *,
    repo: str,
    pr_number: int,
    head_sha: str,
    body: str,
    inline_findings: list[dict[str, Any]],
    event: str = "COMMENT",
) -> bool:
    """POST /repos/{owner}/{repo}/pulls/{pr_number}/reviews

    Submits a single PR review event bundling the summary body + every
    inline-routed finding as a `comments[]` entry.

    Args:
      repo:             "owner/name"
      pr_number:        the PR number
      head_sha:         commit SHA to anchor the review against
      body:             the formatted review body — verdict line + LLM
                        verdict prose + (optional) "Other findings" list
                        for summary-routed findings. Total length is
                        bounded only by GitHub's PR review body limit
                        (currently ~65 KB); the LLM verdict piece is
                        capped via `truncate_summary` upstream. Multiple
                        summary-routed Nits/Lows can easily push the full
                        body past 600 chars even though the LLM verdict
                        itself is capped — this arg is the FULL body, not
                        just the verdict.
      inline_findings:  list of dicts already rendered for inline use.
                        Each dict must have keys `path`, `line`, `side`,
                        `body`. `line` is the FILE line number (not the
                        deprecated `position`/diff-offset); `side` is
                        "RIGHT" for added/modified lines (the only side
                        we comment on — `parse_diff_positions` already
                        filters to right-side `+` lines).
      event:            "COMMENT" (default — advisory; doesn't count toward
                        "Require approvals" gates) or "APPROVE" (bot
                        approves the PR; counts toward branch protection's
                        approval count) or "REQUEST_CHANGES" (blocks merge;
                        currently unused — we prefer the exit-non-zero
                        workflow gate for blocking findings).

    Returns True on success, False with a `::warning::` log on failure.

    Implementation: the payload goes in via `--input -` (stdin) as
    JSON to avoid argv-length issues when many inline findings stack
    up.
    """
    payload: dict[str, Any] = {
        "commit_id": head_sha,
        "event": event,
        "body": body,
        "comments": [
            {
                "path": f["path"],
                # `line` + `side` is the current GitHub API shape for
                # PR review inline comments. The deprecated `position`
                # field rejects with 422 on `POST /pulls/{n}/reviews`
                # when the value is a file line number (which it
                # almost always is in practice).
                "line": f["line"],
                "side": f.get("side", "RIGHT"),
                "body": f["body"],
            }
            for f in inline_findings
        ],
    }
    payload_json = json.dumps(payload)
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{repo}/pulls/{pr_number}/reviews",
            "--input", "-",
        ],
        input=payload_json,
        check=False, capture_output=True, text=True,
    )
    if result.returncode != 0:
        # `gh api` writes the GitHub error JSON to STDOUT and a short
        # "gh: Unprocessable Entity (HTTP 422)" line to STDERR. The
        # actionable detail (which field failed validation, the
        # specific error code) is in stdout. Log BOTH, untruncated, so
        # we can diagnose 422s without re-running with extra
        # instrumentation. The 4 KB cap is well over GitHub's typical
        # error-body size (~200-500 bytes) and exists only as a
        # workflow-log hygiene bound.
        _LOG_TAIL = 4000
        print(
            f"::warning::post_pr_review failed pr={pr_number} "
            f"comments={len(payload['comments'])} "
            f"rc={result.returncode}",
            file=sys.stderr,
        )
        stderr_text = (result.stderr or "").strip()
        stdout_text = (result.stdout or "").strip()
        if stderr_text:
            print(
                f"::warning::post_pr_review stderr: {stderr_text[:_LOG_TAIL]}",
                file=sys.stderr,
            )
        if stdout_text:
            print(
                f"::warning::post_pr_review stdout (GitHub error JSON): "
                f"{stdout_text[:_LOG_TAIL]}",
                file=sys.stderr,
            )
        return False

    # Surface the created review's id + how many comments GitHub accepted
    # so a degraded post (200 but missing comments) is still visible in
    # the workflow log. GitHub's response body is the created review
    # object; on the off chance it's not JSON, fall back to a generic
    # success notice instead of failing the run.
    try:
        review = json.loads(result.stdout)
        review_id = review.get("id")
        # GitHub doesn't echo back the inline comments inline in the
        # review object; the count we sent IS the count it accepted
        # (per-comment 422s would have failed the whole transaction).
        accepted = len(payload["comments"])
        print(
            f"::notice::posted review id={review_id} comments={accepted}",
            file=sys.stderr,
        )
    except (ValueError, TypeError):
        print(
            "::notice::posted review (response not JSON)",
            file=sys.stderr,
        )
    return True
