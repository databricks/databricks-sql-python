"""Pure tree-walking helpers for GitHub PR review comments.

Network I/O is the caller's responsibility — pass the list of comment
dicts in. Each comment dict must have `id`, `in_reply_to_id` (None for
roots), `body`, `created_at`. The shape matches what `gh api
.../pulls/N/comments` returns.
"""
from __future__ import annotations

from typing import Any, Callable, Optional


def find_root(comments: list[dict[str, Any]], target_id: int) -> Optional[dict[str, Any]]:
    by_id = {c["id"]: c for c in comments}
    cur = by_id.get(target_id)
    if cur is None:
        return None
    seen: set[int] = set()
    while True:
        if cur["id"] in seen:
            return None
        seen.add(cur["id"])
        parent_id = cur.get("in_reply_to_id")
        if parent_id is None or parent_id not in by_id:
            return cur
        cur = by_id[parent_id]


def walk_thread(comments: list[dict[str, Any]], root_id: int) -> list[dict[str, Any]]:
    by_id = {c["id"]: c for c in comments}
    if root_id not in by_id:
        return []
    children: dict[int, list[int]] = {}
    for c in comments:
        parent = c.get("in_reply_to_id")
        if parent is not None:
            children.setdefault(parent, []).append(c["id"])

    out: list[dict[str, Any]] = []
    stack = [root_id]
    seen: set[int] = set()
    while stack:
        cid = stack.pop()
        if cid in seen:
            continue
        seen.add(cid)
        out.append(by_id[cid])
        stack.extend(children.get(cid, []))
    out.sort(key=lambda c: c.get("created_at", ""))
    return out


def find_unaddressed_threads(
    comments: list[dict[str, Any]],
    *,
    is_bot_reply: Callable[[dict[str, Any]], bool],
    bot_login_prefix: str,
    max_replies_per_thread: int,
    is_bot_authored: Optional[Callable[[dict[str, Any]], bool]] = None,
) -> list[list[dict[str, Any]]]:
    """Return threads that engineer-bot still needs to engage with.

    A thread is "unaddressed" when ALL of:
      - Its root was NOT authored by engineer-bot (skip the bot's own
        comments — defense in depth, today the bot never opens a
        review thread).
      - The total count of bot-replies in the thread is strictly less
        than `max_replies_per_thread`. Bounds cross-bot ping-pong.
      - There is NO prior bot reply, OR the latest non-bot comment is
        newer than the latest bot reply. The second clause handles
        "I replied; reviewer pushed back; I should look again."
        Without it the bot would re-process every thread on every
        run; with only "no prior bot reply" we'd never re-engage on
        a contested thread.

    Each returned element is the thread's comments in creation order
    (matching `walk_thread`'s output). The list of threads is sorted
    by root `created_at` so callers process oldest-first.

    Args:
      comments: full PR review-comment list (e.g. from
        `github_ops.list_pr_review_comments`).
      is_bot_reply: predicate returning True if a comment counts
        toward `max_replies_per_thread`. Substantive bot replies
        only — typically EXCLUDES infrastructure notices (e.g.
        `action=error` crash replies) so transient crashes don't
        consume the cap.
      bot_login_prefix: prefix of engineer-bot's GitHub user login
        (e.g. `"peco-engineer-bot"`). Threads whose ROOT login starts
        with this prefix are skipped.
      max_replies_per_thread: hard cap on bot replies per thread.
      is_bot_authored: predicate returning True if a comment is from
        the bot AT ALL (regardless of substantive vs infrastructure
        kind). Used to identify the reviewer ask via the
        "latest comment NOT from the bot" rule. When omitted,
        defaults to `is_bot_reply` for back-compat. Engineer-bot
        callers should pass a separate predicate that DOES match
        error replies — an `action=error` reply is still "from the
        bot," and treating it as a fresh reviewer ask causes
        spurious re-engagement on transient crashes.
    """
    if is_bot_authored is None:
        is_bot_authored = is_bot_reply
    if not comments:
        return []
    roots: list[dict[str, Any]] = [
        c for c in comments if c.get("in_reply_to_id") is None
    ]
    out: list[list[dict[str, Any]]] = []
    for root in roots:
        root_login = (root.get("user") or {}).get("login") or ""
        if root_login.startswith(bot_login_prefix):
            continue
        thread = walk_thread(comments, root["id"])
        if not thread:
            continue
        bot_replies = [c for c in thread if is_bot_reply(c)]
        if len(bot_replies) >= max_replies_per_thread:
            continue
        if not bot_replies:
            out.append(thread)
            continue
        # Re-engagement: latest non-bot comment AFTER latest bot reply.
        # `non_bot` filters via `is_bot_authored` (not `is_bot_reply`)
        # so an `action=error` reply that DOESN'T count toward the
        # cap still counts as bot-authored — preventing the bot's own
        # crash notice from being misclassified as a fresh reviewer
        # ask.
        non_bot = [c for c in thread if not is_bot_authored(c)]
        if not non_bot:
            # Every comment in the thread is bot-markered (e.g. root
            # contains a marker substring quoted from review text, or
            # a reviewer's comment was deleted). No reviewer ask to
            # react to — skip explicitly rather than relying on
            # empty-string ordering against an ISO timestamp.
            continue
        # Compare by (created_at, id) so threads with same-second
        # timestamps (a real possibility during a multi-comment review
        # burst) tie-break on monotonic GitHub comment IDs — the later
        # ID is reliably the later comment. Strict `>` on ISO strings
        # alone would skip a reviewer reply posted in the same second
        # as the bot's reply ("not after, so stale").
        def _key(c):
            return (c.get("created_at", ""), c.get("id", 0))
        latest_bot_key = max(_key(c) for c in bot_replies)
        latest_non_bot_key = max(_key(c) for c in non_bot)
        if latest_non_bot_key > latest_bot_key:
            out.append(thread)
    out.sort(key=lambda t: t[0].get("created_at", ""))
    return out
