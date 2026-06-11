"""Shared review-thread helpers (formerly the reconcile module).

This module now holds GraphQL/REST helpers used by both the LLM-driven
P1 dedup context (`gather_context.format_open_threads` consumes
`fetch_open_review_threads`) and the followup-trigger workflow
(`reviewer_bot/followup.py` calls `auto_resolve`, `post_inline_reply`,
and `extract_finding_id_from_body`).

The original "reconcile stale threads" loop — which walked OPEN
threads at the end of every review run and resolved findings whose
issues had been fixed since the prior run — has been removed.
Followup.py handles that closing-the-loop work with higher accuracy:
when engineer-bot pushes a fix and posts `Pushed <sha>`, the
`pull_request_review_comment` event fires followup, which SHA-verifies
the claimed fix against the original finding and emits
`<retract>`/`<agree_and_suggest>` (both of which call `auto_resolve`).

Filename is preserved (rather than renamed to `thread_ops.py`) to
keep import churn out of this PR — a future cleanup can rename if
desired. The functions defined here are pure infrastructure now;
nothing in this file makes the "is this finding fixed?" judgment.

INVARIANT (load-bearing):
    Every `resolveReviewThread` mutation MUST be preceded by an
    inline reply explaining why. Silent resolves hide rationale and
    prevent the author from contesting the close. The `auto_resolve`
    helper enforces this — if the reply fails to post, the resolve
    does NOT fire. Followup goes through `auto_resolve` for the same
    reason.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from typing import Callable, Optional, TypedDict

from .markers import (
    has_reconcile_marker,
)


class ReviewThread(TypedDict):
    """Shape of a thread dict returned by `fetch_open_review_threads`.

    This is the contract between this module's GraphQL projection and
    every downstream consumer (`gather_context.format_open_threads`,
    `followup.*`). Without an explicit
    declared shape the contract was implicit: a future change to the
    GraphQL projection that renamed or dropped a field would silently
    degrade consumers — `format_open_threads` would emit
    "(no open review-bot threads)" when `root_author` lookups missed,
    or render `#None` when `root_comment_id` was absent. Declaring the
    keys here lets type-checkers (`mypy --strict`, IDE inspections)
    flag mismatches at the boundary instead of at runtime — and the
    TypedDict name itself serves as the searchable anchor for "what
    fields can a thread dict have".

    Keep this in sync with the dict literal built inside
    `fetch_open_review_threads` — adding a key in one place without
    the other is the exact failure mode the TypedDict guards against.
    `format_open_threads` also performs a defensive presence check at
    the boundary so a runtime mismatch (e.g. a malformed GraphQL
    payload bypassing the type-check) surfaces as a logged warning
    rather than a silent empty section.
    """
    thread_id: str
    is_resolved: bool
    root_comment_id: Optional[int]
    root_body: str
    root_path: Optional[str]
    root_line: Optional[int]
    root_author: str
    root_created_at: str
    has_existing_reconcile_reply: bool
    comments_truncated: bool


# Required keys that downstream consumers (`format_open_threads`,
# `followup.*`) rely on. Used by the boundary
# assertion in `format_open_threads` to catch a contract drift if
# the GraphQL projection changes a key name without updating
# `ReviewThread` and the dict-literal in `fetch_open_review_threads`.
REVIEW_THREAD_REQUIRED_KEYS: frozenset[str] = frozenset({
    "root_author",
    "root_comment_id",
    "root_path",
    "root_line",
    "root_body",
    "root_created_at",
})


def _run_gh(args: list[str]) -> str:
    """Same minimal helper as run_review._run_gh — local copy keeps the
    module standalone (no cross-module private imports) and lets unit
    tests monkeypatch `subprocess.run` on this module specifically."""
    result = subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True,
    )
    return result.stdout


# ─── GraphQL helpers ───────────────────────────────────────────────────


_REVIEW_THREADS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          comments(first: 100) {
            pageInfo { hasNextPage }
            nodes {
              databaseId
              body
              path
              line
              originalLine
              author { login }
              createdAt
            }
          }
        }
      }
    }
  }
}
"""
# Fetch up to 100 comments per thread. The reconcile loop needs to
# see ALL replies to detect threads where it has already posted a
# `reconcile` reply on a prior run — without that visibility, a
# failing `resolveReviewThread` mutation would cause every reconcile
# pass to re-post the same "Resolved — on second look this isn't an
# issue." reply, piling up duplicates indefinitely.
#
# 100 is GitHub's documented per-page max for this connection. We
# don't paginate INSIDE a thread (the alternative would be a second
# GraphQL round-trip per long thread). When a thread genuinely
# exceeds 100 comments, `pageInfo.hasNextPage` is True and
# `fetch_open_review_threads` falls back to a "marker probably
# exists" assumption to preserve the duplicate-reply defense — see
# the parsing block below.


def fetch_open_review_threads(
    *, repo: str, pr_number: int,
) -> list[ReviewThread]:
    """GraphQL: return all OPEN review threads on the PR with their root
    comment. Each returned dict matches the `ReviewThread` TypedDict::

        {
            "thread_id":       <GraphQL global id>,
            "is_resolved":     False,         # filtered to False below
            "root_comment_id": <REST databaseId>,
            "root_body":       "<body text>",
            "root_path":       "src/foo.py",  # may be None
            "root_line":       42,            # may be None
            "root_author":     "peco-review-bot[bot]",
            "root_created_at": "<ISO timestamp>",
        }

    Resolved threads are filtered out before returning — reconcile only
    acts on OPEN threads. (A resolved thread by definition was already
    closed by someone; re-resolving is a no-op.)

    Paginates until exhausted; the 100-thread page size matches the
    GraphQL max and is fine for any PR that doesn't have hundreds of
    review threads.
    """
    owner, name = repo.split("/", 1)
    cursor: Optional[str] = None
    out: list[ReviewThread] = []
    while True:
        # Variables are passed via -F flags on the gh CLI, not as a
        # separate JSON variables block — gh handles the bind for us.
        cmd = [
            "api", "graphql",
            "-f", f"query={_REVIEW_THREADS_QUERY}",
            "-F", f"owner={owner}",
            "-F", f"name={name}",
            "-F", f"number={pr_number}",
        ]
        if cursor is not None:
            cmd += ["-F", f"cursor={cursor}"]
        raw = _run_gh(cmd)
        data = json.loads(raw)
        threads_block = (
            (((data.get("data") or {}).get("repository") or {})
             .get("pullRequest") or {})
            .get("reviewThreads") or {}
        )
        nodes = threads_block.get("nodes") or []
        for n in nodes:
            if n.get("isResolved"):
                continue
            comments_block = n.get("comments") or {}
            comments = comments_block.get("nodes") or []
            if not comments:
                continue
            root = comments[0]
            author = (root.get("author") or {}).get("login") or ""
            # Defense in depth against failing resolve mutations:
            # surface whether ANY comment in the thread already carries
            # the reconcile marker. Callers use this STRICT signal to
            # decide whether the post-before-resolve invariant is
            # already satisfied by an existing reply (and thus whether
            # the retry-only resolve path is safe to take).
            #
            # "Strict" matters: this flag drives a direct call to
            # `graphql_resolve_thread` that bypasses `auto_resolve`,
            # justified ONLY by the precondition "a reply with the
            # reconcile marker is already on the thread". A false
            # positive here would resolve a thread with no audit-trail
            # reply ever posted.
            marker_observed_in_replies = any(
                has_reconcile_marker(c.get("body") or "")
                for c in comments[1:]   # skip root; only check replies
            )
            # Pathological case: thread has >100 comments. We didn't
            # fetch all of them — the page-cap is 100. Surface the
            # truncation as a SEPARATE flag rather than folding it
            # into the marker signal: the marker check is still strict
            # (False = "not observed"), and downstream code can use
            # `comments_truncated` to choose the safer fallback (full
            # post-then-resolve path) when uncertain. Worst case under
            # this approach: a long thread whose marker IS in the
            # truncated tail gets one duplicate reply per affected run
            # until the resolve succeeds — strictly better than a
            # silent resolve with no audit trail.
            comments_page = comments_block.get("pageInfo") or {}
            comments_truncated = bool(comments_page.get("hasNextPage"))
            out.append({
                "thread_id": n.get("id"),
                "is_resolved": bool(n.get("isResolved")),
                "root_comment_id": root.get("databaseId"),
                "root_body": root.get("body") or "",
                "root_path": root.get("path"),
                "root_line": root.get("line") or root.get("originalLine"),
                "root_author": author,
                "root_created_at": root.get("createdAt") or "",
                # Strict: True iff the marker was observed in a fetched
                # reply. Use this to decide whether retry-only resolve
                # is safe (the prior reply satisfies post-before-resolve).
                "has_existing_reconcile_reply": marker_observed_in_replies,
                # Observability: True iff the comment list was truncated
                # at the 100-cap. Callers may use this to log or to
                # choose a safer path; it is NEVER a substitute for the
                # strict marker signal.
                "comments_truncated": comments_truncated,
            })
        page = threads_block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if not cursor:
            break
    return out


def graphql_resolve_thread(thread_id: str) -> bool:
    """Resolve a review thread by GraphQL global id. Returns True on
    success, False on failure (logged as a `::warning::`). Never raises:
    `subprocess.run(check=False)` + return-code check means transient
    network errors and auth failures are surfaced as False, not an
    exception that would abort the surrounding loop.

    DO NOT call this directly — go through `auto_resolve` so the
    INVARIANT (post-before-resolve) is enforced for every code path.
    No sanctioned direct callers remain after the reconcile loop was
    removed; if you find yourself wanting to call this without going
    through `auto_resolve`, you almost certainly want a wrapper that
    posts an explanatory reply first.

    Required permission: the App's installation must have
    `Contents: Read and Write` (counter-intuitively, NOT
    `Pull requests: write` — see GitHub community discussion #44650).
    Without this, the mutation returns "Resource not accessible by
    integration" regardless of which token is used. The resolve
    event shows in the timeline under the App identity (e.g.
    `peco-review-bot[bot] marked as resolved`).
    """
    mutation = (
        "mutation($id: ID!) { "
        "resolveReviewThread(input: {threadId: $id}) { "
        "thread { id isResolved } } }"
    )
    result = subprocess.run(
        ["gh", "api", "graphql",
         "-f", f"query={mutation}",
         "-F", f"id={thread_id}"],
        check=False, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(
            f"::warning::graphql_resolve_thread failed thread_id={thread_id} "
            f"rc={result.returncode}: {result.stderr.strip()[:200]}",
            file=sys.stderr,
        )
        return False
    return True


def post_inline_reply(
    *, repo: str, pr_number: int, root_comment_id: int, body: str,
) -> Optional[int]:
    """POST /repos/{owner}/{repo}/pulls/{pr_number}/comments/{root_id}/replies

    Replies are anchored to the root comment of the thread. Returns the
    new comment id on success, None on failure (logged as a `::warning::`).
    """
    result = subprocess.run(
        ["gh", "api",
         f"repos/{repo}/pulls/{pr_number}/comments/{root_comment_id}/replies",
         "-f", f"body={body}",
         "--jq", ".id"],
        check=False, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(
            f"::warning::post_inline_reply failed root_id={root_comment_id} "
            f"rc={result.returncode}: {result.stderr.strip()[:200]}",
            file=sys.stderr,
        )
        return None
    try:
        return int(result.stdout.strip())
    except ValueError:
        print(
            f"::warning::post_inline_reply returned non-int id for "
            f"root_id={root_comment_id}: {result.stdout!r}",
            file=sys.stderr,
        )
        return None


# ─── Hard invariant: post-then-resolve ─────────────────────────────────


def auto_resolve(
    *,
    repo: str,
    pr_number: int,
    thread_id: str,
    root_comment_id: int,
    reason_body: str,
    reply_fn: Optional[
        Callable[..., Optional[int]]
    ] = None,
    resolve_fn: Optional[Callable[[str], bool]] = None,
) -> bool:
    """INVARIANT: every resolve MUST be preceded by an inline reply.

    Silent resolves hide rationale and prevent the author from contesting
    the close. If `reply_fn` fails to post, this function does NOT call
    `resolve_fn`.

    Returns True iff BOTH reply and resolve succeeded.

    `reply_fn` and `resolve_fn` are injectable for unit testing; the
    real call sites pass None and the helper uses the module-level
    `post_inline_reply` / `graphql_resolve_thread`.

    Caller is responsible for embedding the appropriate marker
    (RECONCILE_MARKER or a follow-up marker) inside `reason_body` —
    auto_resolve is action-agnostic so Update 1 and Update 2 share it.
    """
    if reply_fn is None:
        reply_fn = post_inline_reply
    if resolve_fn is None:
        resolve_fn = graphql_resolve_thread

    reply_id = reply_fn(
        repo=repo,
        pr_number=pr_number,
        root_comment_id=root_comment_id,
        body=reason_body,
    )
    if reply_id is None:
        # Reply failed → do NOT resolve. Leaving the thread open is
        # the safer default — the author still sees the original
        # finding and can decide what to do. A silent resolve here
        # would erase the bot's reasoning trail.
        print(
            f"::warning::auto_resolve aborted: reply failed for "
            f"thread_id={thread_id} (skipping resolve to preserve "
            f"invariant)",
            file=sys.stderr,
        )
        return False
    return resolve_fn(thread_id)


# ─── Finding-id extraction ─────────────────────────────────────────────


_INLINE_ID_RE = re.compile(
    r"<!-- pr-review-bot:v1 type=inline id=([A-Za-z0-9_.\-]+) -->"
)


def extract_finding_id_from_body(body: str) -> Optional[str]:
    """Pull the embedded finding id out of an inline marker.

    Returns None if no marker is present (e.g., the body isn't a bot
    finding — caller is expected to filter first).
    """
    if not isinstance(body, str):
        return None
    m = _INLINE_ID_RE.search(body)
    return m.group(1) if m else None




