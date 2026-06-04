"""Tests for shared.threads — review-comment tree walking."""
from __future__ import annotations

from scripts.shared import threads


def _c(cid: int, in_reply_to=None, body=""):
    return {
        "id": cid,
        "in_reply_to_id": in_reply_to,
        "body": body,
        "created_at": f"2026-01-01T00:00:{cid:02d}Z",
    }


class TestFindRoot:
    def test_single_comment_is_its_own_root(self):
        assert threads.find_root([_c(1)], target_id=1)["id"] == 1

    def test_two_level_reply_walks_to_root(self):
        comments = [_c(1), _c(2, in_reply_to=1)]
        assert threads.find_root(comments, target_id=2)["id"] == 1

    def test_deep_chain_walks_to_root(self):
        comments = [_c(1), _c(2, 1), _c(3, 2), _c(4, 3)]
        assert threads.find_root(comments, target_id=4)["id"] == 1

    def test_unknown_target_returns_none(self):
        assert threads.find_root([_c(1)], target_id=99) is None

    def test_cycle_does_not_infinite_loop(self):
        comments = [_c(2, 3), _c(3, 2)]
        assert threads.find_root(comments, target_id=2) is None


class TestWalkThread:
    def test_root_with_no_replies(self):
        result = threads.walk_thread([_c(1)], root_id=1)
        assert [c["id"] for c in result] == [1]

    def test_root_with_two_direct_replies(self):
        comments = [_c(1), _c(2, 1), _c(3, 1)]
        assert sorted(c["id"] for c in threads.walk_thread(comments, 1)) == [1, 2, 3]

    def test_nested_replies_included(self):
        comments = [_c(1), _c(2, 1), _c(3, 2), _c(4, 3)]
        assert sorted(c["id"] for c in threads.walk_thread(comments, 1)) == [1, 2, 3, 4]

    def test_replies_are_chronological(self):
        comments = [
            _c(1),
            {**_c(3, 1), "created_at": "2026-01-01T00:00:05Z"},
            {**_c(2, 1), "created_at": "2026-01-01T00:00:10Z"},
        ]
        assert [c["id"] for c in threads.walk_thread(comments, 1)] == [1, 3, 2]

    def test_other_thread_excluded(self):
        comments = [_c(1), _c(2, 1), _c(10), _c(11, 10)]
        assert sorted(c["id"] for c in threads.walk_thread(comments, 1)) == [1, 2]


def _c2(cid, *, in_reply_to=None, body="", user="alice", created_at=None):
    return {
        "id": cid,
        "in_reply_to_id": in_reply_to,
        "body": body,
        "created_at": created_at or f"2026-01-01T00:00:{cid:02d}Z",
        "user": {"login": user},
    }


def _is_eng_bot(c):
    return "<!-- engineer-bot-csharp-coverage:v1" in (c.get("body") or "")


class TestFindUnaddressedThreads:
    def test_empty_input(self):
        assert threads.find_unaddressed_threads(
            [],
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        ) == []

    def test_fresh_thread_with_no_bot_reply_is_returned(self):
        comments = [_c2(1, user="peco-review-bot[bot]", body="finding")]
        result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert len(result) == 1
        assert [c["id"] for c in result[0]] == [1]

    def test_thread_with_bot_reply_and_no_newer_human_is_skipped(self):
        comments = [
            _c2(1, user="peco-review-bot[bot]", body="finding",
                created_at="2026-01-01T00:00:01Z"),
            _c2(2, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="fixed\n<!-- engineer-bot-csharp-coverage:v1 followup action=fix -->",
                created_at="2026-01-01T00:00:02Z"),
        ]
        assert threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        ) == []

    def test_reviewer_replies_after_bot_reopens_thread(self):
        comments = [
            _c2(1, user="peco-review-bot[bot]", body="finding",
                created_at="2026-01-01T00:00:01Z"),
            _c2(2, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="fixed\n<!-- engineer-bot-csharp-coverage:v1 followup action=fix -->",
                created_at="2026-01-01T00:00:02Z"),
            _c2(3, in_reply_to=2, user="eric",
                body="not quite — also need X",
                created_at="2026-01-01T00:00:03Z"),
        ]
        result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert len(result) == 1
        assert [c["id"] for c in result[0]] == [1, 2, 3]

    def test_thread_at_cap_is_skipped_even_with_newer_human_reply(self):
        comments = [
            _c2(1, user="peco-review-bot[bot]",
                created_at="2026-01-01T00:00:01Z"),
            _c2(2, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="r1\n<!-- engineer-bot-csharp-coverage:v1 -->",
                created_at="2026-01-01T00:00:02Z"),
            _c2(3, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="r2\n<!-- engineer-bot-csharp-coverage:v1 -->",
                created_at="2026-01-01T00:00:03Z"),
            _c2(4, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="r3\n<!-- engineer-bot-csharp-coverage:v1 -->",
                created_at="2026-01-01T00:00:04Z"),
            _c2(5, in_reply_to=1, user="eric", body="keep going",
                created_at="2026-01-01T00:00:05Z"),
        ]
        assert threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        ) == []

    def test_thread_whose_root_is_engineer_bot_is_skipped(self):
        comments = [
            _c2(1, user="peco-engineer-bot[bot]",
                body="engineer-bot-opened review thread (defense in depth)"),
            _c2(2, in_reply_to=1, user="peco-review-bot[bot]", body="reply"),
        ]
        assert threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        ) == []

    def test_multiple_threads_returned_oldest_first(self):
        comments = [
            _c2(10, user="peco-review-bot[bot]", body="finding A",
                created_at="2026-01-01T10:00:00Z"),
            _c2(20, user="peco-review-bot[bot]", body="finding B",
                created_at="2026-01-01T09:00:00Z"),
            _c2(30, user="Copilot", body="finding C",
                created_at="2026-01-01T11:00:00Z"),
        ]
        result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert [t[0]["id"] for t in result] == [20, 10, 30]

    def test_thread_unrelated_to_bot_marker_is_returned(self):
        """A human reviewer's thread (no bot involvement) is just as
        unaddressed as a peco-review-bot one — engineer-bot reacts to
        all reviewers, not only bot reviewers."""
        comments = [_c2(1, user="alice", body="please refactor X")]
        result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert len(result) == 1
        assert result[0][0]["id"] == 1

    def test_thread_with_only_bot_markered_comments_is_skipped(self):
        """If every comment in a thread carries the bot marker (e.g.
        a non-bot login posted text that quotes one of the bot's prior
        marker strings), there's no actual reviewer ask. The explicit
        non-bot guard skips this rather than relying on `""` vs ISO
        timestamp string ordering working out to False by accident."""
        comments = [
            _c2(1, user="alice",
                body="reposted: <!-- engineer-bot-csharp-coverage:v1 -->",
                created_at="2026-01-01T00:00:01Z"),
        ]
        # is_bot_reply matches on marker → the only comment is a bot
        # reply by predicate → no non-bot comments → skip.
        assert threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        ) == []

    def test_same_second_timestamps_tiebreak_by_id(self):
        """Burst review: bot replies and reviewer posts in the same
        second. ISO timestamps tie; the later GitHub comment ID is
        the later comment. Re-engagement should fire on the reviewer
        reply even when `created_at` strings are equal."""
        comments = [
            _c2(1, user="peco-review-bot[bot]", body="finding",
                created_at="2026-01-01T00:00:00Z"),
            _c2(2, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="fix\n<!-- engineer-bot-csharp-coverage:v1 -->",
                created_at="2026-01-01T00:00:05Z"),
            _c2(3, in_reply_to=2, user="eric",
                body="not quite",
                created_at="2026-01-01T00:00:05Z"),
        ]
        result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_is_eng_bot,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert len(result) == 1, "id=3 reply (same second, later id) should re-open"

    def test_is_bot_authored_excludes_error_replies_from_non_bot(self):
        """If `is_bot_reply` excludes \`action=error\` replies (the
        engineer-bot pattern) but we use it for BOTH cap-counting AND
        the non_bot timestamp comparison, an error reply gets treated
        as a fresh reviewer ask and the bot re-engages on its own
        crash notice. The separate `is_bot_authored` predicate fixes
        this — it matches error replies too, so they don't appear
        in `non_bot`."""
        # Eng-bot predicates that mimic the production pattern:
        # `_is_bot_reply` excludes action=error; `_is_bot_authored`
        # includes it.
        def _eng_authored(c):
            return "<!-- engineer-bot-csharp-coverage:v1" in (c.get("body") or "")
        def _eng_reply(c):
            if not _eng_authored(c):
                return False
            return "action=error" not in (c.get("body") or "")

        comments = [
            _c2(1, user="peco-review-bot[bot]", body="finding",
                created_at="2026-01-01T00:00:01Z"),
            _c2(2, in_reply_to=1, user="peco-engineer-bot[bot]",
                body="fixed\n<!-- engineer-bot-csharp-coverage:v1 followup action=fix -->",
                created_at="2026-01-01T00:00:02Z"),
            _c2(3, in_reply_to=2, user="peco-engineer-bot[bot]",
                body="crashed\n<!-- engineer-bot-csharp-coverage:v1 followup action=error -->",
                created_at="2026-01-01T00:00:03Z"),
        ]
        # Without passing is_bot_authored, the bug WOULD show: the
        # error reply at id=3 is classified as non_bot (per
        # _eng_reply) — its timestamp is the latest of any non-bot
        # comment — so non_bot_time > bot_time and the thread
        # re-opens on the bot's own crash.
        bug_result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_eng_reply,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert len(bug_result) == 1, "demonstrates the bug — wrong re-engage"
        # With is_bot_authored, the error reply is correctly classified
        # as bot-authored → not in non_bot → thread stays skipped.
        fixed_result = threads.find_unaddressed_threads(
            comments,
            is_bot_reply=_eng_reply,
            is_bot_authored=_eng_authored,
            bot_login_prefix="peco-engineer-bot",
            max_replies_per_thread=3,
        )
        assert fixed_result == [], "fixed — error reply is bot-authored, no re-engage"
