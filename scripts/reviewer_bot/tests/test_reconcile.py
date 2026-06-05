"""Unit tests for reconcile.py (Phase 2 Update 1).

The hard invariant under test: every `resolveReviewThread` mutation
MUST be preceded by an inline reply. If the reply fails, the resolve
must NOT fire.
"""
from __future__ import annotations

from typing import Any

import pytest

from scripts.reviewer_bot import reconcile as rc
from scripts.reviewer_bot.markers import (
    RECONCILE_MARKER,
    FOLLOWUP_MARKER_PREFIX,
)


# ─── Helpers / fixtures ────────────────────────────────────────────────


def _make_thread(
    *,
    thread_id: str = "T1",
    is_resolved: bool = False,
    root_comment_id: int = 100,
    root_body: str = "🔴 Critical — issue\n<!-- pr-review-bot:v1 type=inline id=F1 -->",
    root_path: str = "src/foo.py",
    root_line: int = 42,
    root_author: str = "peco-review-bot[bot]",
    root_created_at: str = "2026-05-01T10:00:00Z",
    has_existing_reconcile_reply: bool = False,
) -> dict[str, Any]:
    return {
        "thread_id": thread_id,
        "is_resolved": is_resolved,
        "root_comment_id": root_comment_id,
        "root_body": root_body,
        "root_path": root_path,
        "root_line": root_line,
        "root_author": root_author,
        "root_created_at": root_created_at,
        "has_existing_reconcile_reply": has_existing_reconcile_reply,
    }


def _make_finding(
    *, id: str = "F1", file: str = "src/foo.py", line: int = 42,
    body: str = "the missing await keyword causes a race condition",
) -> dict[str, Any]:
    return {"id": id, "file": file, "line": line, "body": body}


@pytest.fixture
def call_order():
    """List that test fns push call-tags onto, in execution order.
    Used to verify the post-then-resolve invariant."""
    return []


# ─── Marker extraction ─────────────────────────────────────────────────


class TestExtractFindingId:
    def test_extracts_id_from_inline_marker(self):
        body = "stuff\n<!-- pr-review-bot:v1 type=inline id=F42 -->"
        assert rc.extract_finding_id_from_body(body) == "F42"

    def test_returns_none_when_no_marker(self):
        assert rc.extract_finding_id_from_body("plain text") is None

    def test_returns_none_for_non_string(self):
        assert rc.extract_finding_id_from_body(None) is None  # type: ignore[arg-type]


# ─── auto_resolve invariant ────────────────────────────────────────────


class TestAutoResolveInvariant:
    def test_posts_before_resolving(self, call_order):
        """Hard invariant: reply MUST be posted before resolve is called."""
        def reply_fn(**kwargs):
            call_order.append("reply")
            return 999

        def resolve_fn(thread_id):
            call_order.append("resolve")
            return True

        ok = rc.auto_resolve(
            repo="o/r", pr_number=1, thread_id="T1", root_comment_id=100,
            reason_body="x",
            reply_fn=reply_fn, resolve_fn=resolve_fn,
        )
        assert ok is True
        # Order MUST be reply first, then resolve.
        assert call_order == ["reply", "resolve"]

    def test_skips_resolve_when_reply_fails(self, call_order):
        """If post_inline_reply returns None, graphql_resolve_thread MUST NOT fire."""
        def reply_fn(**kwargs):
            call_order.append("reply")
            return None  # signal failure

        def resolve_fn(thread_id):
            call_order.append("resolve")  # should never run
            return True

        ok = rc.auto_resolve(
            repo="o/r", pr_number=1, thread_id="T1", root_comment_id=100,
            reason_body="x",
            reply_fn=reply_fn, resolve_fn=resolve_fn,
        )
        assert ok is False
        # ONLY the reply attempt — no resolve call after the failure.
        assert call_order == ["reply"]

    def test_returns_false_when_resolve_fails(self, call_order):
        """Reply succeeded, resolve failed → return False (caller may
        retry / observability needs to know)."""
        def reply_fn(**kwargs):
            call_order.append("reply")
            return 999

        def resolve_fn(thread_id):
            call_order.append("resolve")
            return False

        ok = rc.auto_resolve(
            repo="o/r", pr_number=1, thread_id="T1", root_comment_id=100,
            reason_body="x",
            reply_fn=reply_fn, resolve_fn=resolve_fn,
        )
        assert ok is False
        assert call_order == ["reply", "resolve"]

    def test_uses_module_defaults_when_no_injection(self, monkeypatch):
        """Wiring sanity: with no injected fns, calls the module-level
        post_inline_reply and graphql_resolve_thread."""
        seen = {"reply": False, "resolve": False}

        def fake_reply(**kwargs):
            seen["reply"] = True
            return 42

        def fake_resolve(thread_id):
            seen["resolve"] = True
            return True

        monkeypatch.setattr(rc, "post_inline_reply", fake_reply)
        monkeypatch.setattr(rc, "graphql_resolve_thread", fake_resolve)

        rc.auto_resolve(
            repo="o/r", pr_number=1, thread_id="T1", root_comment_id=100,
            reason_body="x",
        )
        assert seen == {"reply": True, "resolve": True}



# ─── fetch_open_review_threads (GraphQL query plumbing) ────────────────


class TestFetchOpenReviewThreads:
    def test_filters_out_resolved_threads(self, monkeypatch):
        """Resolved threads in the GraphQL response are dropped."""
        fake_resp = {
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "pageInfo": {
                                "hasNextPage": False,
                                "endCursor": None,
                            },
                            "nodes": [
                                {
                                    "id": "TR1",
                                    "isResolved": False,
                                    "comments": {"nodes": [{
                                        "databaseId": 100,
                                        "body": "open finding",
                                        "path": "src/foo.py",
                                        "line": 10,
                                        "author": {"login": "peco-review-bot[bot]"},
                                        "createdAt": "2026-05-01T10:00:00Z",
                                    }]},
                                },
                                {
                                    "id": "TR2",
                                    "isResolved": True,  # filtered out
                                    "comments": {"nodes": [{
                                        "databaseId": 200,
                                        "body": "resolved already",
                                        "path": "src/bar.py",
                                        "line": 20,
                                        "author": {"login": "peco-review-bot[bot]"},
                                        "createdAt": "2026-05-02T10:00:00Z",
                                    }]},
                                },
                            ],
                        }
                    }
                }
            }
        }
        monkeypatch.setattr(
            rc, "_run_gh", lambda args: __import__("json").dumps(fake_resp),
        )
        out = rc.fetch_open_review_threads(repo="o/r", pr_number=1)
        assert len(out) == 1
        assert out[0]["thread_id"] == "TR1"

    def test_has_existing_reconcile_reply_set_when_marker_in_reply(self, monkeypatch):
        """When a thread's `comments.nodes` includes a reply body that
        carries `RECONCILE_MARKER`, the returned dict must have
        `has_existing_reconcile_reply=True` so reconcile_resolved_threads
        skips re-posting the explanatory reply (the duplicate-reply
        pile-up the PR is designed to prevent)."""
        fake_resp = {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [{
                "id": "TR_WITH_RECONCILE",
                "isResolved": False,
                "comments": {
                    "pageInfo": {"hasNextPage": False},
                    "nodes": [
                        {"databaseId": 100,
                         "body": ("🔴 issue\n"
                                  "<!-- pr-review-bot:v1 type=inline id=F1 -->"),
                         "path": "src/foo.py", "line": 10,
                         "author": {"login": "peco-review-bot[bot]"},
                         "createdAt": "2026-05-01T10:00:00Z"},
                        {"databaseId": 101,
                         "body": ("Resolved — on second look this isn't an issue.\n\n"
                                  f"{RECONCILE_MARKER}"),
                         "path": "src/foo.py", "line": 10,
                         "author": {"login": "peco-review-bot[bot]"},
                         "createdAt": "2026-05-01T11:00:00Z"},
                    ],
                },
            }],
        }}}}}
        monkeypatch.setattr(
            rc, "_run_gh", lambda args: __import__("json").dumps(fake_resp),
        )
        out = rc.fetch_open_review_threads(repo="o/r", pr_number=1)
        assert len(out) == 1
        assert out[0]["has_existing_reconcile_reply"] is True

    def test_has_existing_reconcile_reply_unset_when_no_marker(self, monkeypatch):
        """Thread with no reconcile-marker reply → flag is False."""
        fake_resp = {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [{
                "id": "TR_FRESH",
                "isResolved": False,
                "comments": {
                    "pageInfo": {"hasNextPage": False},
                    "nodes": [
                        {"databaseId": 100,
                         "body": ("🔴 issue\n"
                                  "<!-- pr-review-bot:v1 type=inline id=F1 -->"),
                         "path": "src/foo.py", "line": 10,
                         "author": {"login": "peco-review-bot[bot]"},
                         "createdAt": "2026-05-01T10:00:00Z"},
                    ],
                },
            }],
        }}}}}
        monkeypatch.setattr(
            rc, "_run_gh", lambda args: __import__("json").dumps(fake_resp),
        )
        out = rc.fetch_open_review_threads(repo="o/r", pr_number=1)
        assert out[0]["has_existing_reconcile_reply"] is False

    def test_has_existing_reconcile_reply_strict_on_pagination(self, monkeypatch):
        """When a thread has >100 comments (the inner-page cap), the
        STRICT marker signal stays False if no marker was observed in
        the fetched comments. Truncation is surfaced separately as
        `comments_truncated=True` so callers can choose the safe path
        (full post-then-resolve) instead of the retry-only path that
        would bypass the post-before-resolve invariant.

        Rationale: the previous fail-safe (folding truncation into
        the marker signal) silently violated the precondition of
        `graphql_resolve_thread`'s sanctioned direct-call site. The
        worst case under the strict approach is one duplicate reply
        per affected long thread until the resolve succeeds, which
        is strictly better than a silent resolve with no audit trail."""
        fake_resp = {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [{
                "id": "TR_LONG",
                "isResolved": False,
                "comments": {
                    "pageInfo": {"hasNextPage": True},   # >100 comments
                    "nodes": [
                        {"databaseId": 100,
                         "body": "🔴 issue\n<!-- pr-review-bot:v1 type=inline id=F1 -->",
                         "path": "src/foo.py", "line": 10,
                         "author": {"login": "peco-review-bot[bot]"},
                         "createdAt": "2026-05-01T10:00:00Z"},
                        # ...no marker visible in first page; the tail
                        # may or may not contain one. The strict flag
                        # says "not observed"; the truncation flag
                        # surfaces the uncertainty.
                    ],
                },
            }],
        }}}}}
        monkeypatch.setattr(
            rc, "_run_gh", lambda args: __import__("json").dumps(fake_resp),
        )
        out = rc.fetch_open_review_threads(repo="o/r", pr_number=1)
        assert out[0]["has_existing_reconcile_reply"] is False
        assert out[0]["comments_truncated"] is True

    def test_paginates_across_pages(self, monkeypatch):
        """When the first page has hasNextPage=True, the helper makes
        a second query with the endCursor and concatenates the nodes."""
        pages = [
            {"data": {"repository": {"pullRequest": {"reviewThreads": {
                "pageInfo": {"hasNextPage": True, "endCursor": "CURSOR1"},
                "nodes": [{
                    "id": "T_A",
                    "isResolved": False,
                    "comments": {"nodes": [{
                        "databaseId": 1, "body": "a", "path": "p", "line": 1,
                        "author": {"login": "x"}, "createdAt": "t",
                    }]},
                }],
            }}}}},
            {"data": {"repository": {"pullRequest": {"reviewThreads": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [{
                    "id": "T_B",
                    "isResolved": False,
                    "comments": {"nodes": [{
                        "databaseId": 2, "body": "b", "path": "p", "line": 2,
                        "author": {"login": "x"}, "createdAt": "t",
                    }]},
                }],
            }}}}},
        ]
        call_count = {"n": 0}

        def fake_run_gh(args):
            i = call_count["n"]
            call_count["n"] += 1
            return __import__("json").dumps(pages[i])

        monkeypatch.setattr(rc, "_run_gh", fake_run_gh)
        out = rc.fetch_open_review_threads(repo="o/r", pr_number=1)
        assert [t["thread_id"] for t in out] == ["T_A", "T_B"]
        assert call_count["n"] == 2


# ─── post_inline_reply ─────────────────────────────────────────────────


class TestPostInlineReply:
    def test_calls_correct_endpoint(self, monkeypatch):
        captured: list[Any] = []

        class _FakeResult:
            stdout = "42"
            returncode = 0
            stderr = ""

        def fake_run(args, **kwargs):
            captured.append(args)
            return _FakeResult()

        monkeypatch.setattr(rc.subprocess, "run", fake_run)
        out = rc.post_inline_reply(
            repo="o/r", pr_number=7, root_comment_id=999, body="hello",
        )
        assert out == 42
        # MUST hit the .../pulls/{pr_number}/comments/{id}/replies endpoint.
        # The pr_number segment is required — without it GitHub returns 404.
        cmd = captured[0]
        assert "repos/o/r/pulls/7/comments/999/replies" in cmd
        assert "body=hello" in " ".join(cmd)

    def test_returns_none_on_nonzero_exit(self, monkeypatch, capsys):
        class _FakeResult:
            stdout = ""
            returncode = 1
            stderr = "422 Unprocessable Entity"

        monkeypatch.setattr(
            rc.subprocess, "run",
            lambda *a, **kw: _FakeResult(),
        )
        out = rc.post_inline_reply(
            repo="o/r", pr_number=7, root_comment_id=999, body="hello",
        )
        assert out is None
        err = capsys.readouterr().err
        assert "::warning::" in err

    def test_returns_none_when_stdout_not_int(self, monkeypatch, capsys):
        class _FakeResult:
            stdout = "not-a-number"
            returncode = 0
            stderr = ""

        monkeypatch.setattr(
            rc.subprocess, "run",
            lambda *a, **kw: _FakeResult(),
        )
        out = rc.post_inline_reply(
            repo="o/r", pr_number=7, root_comment_id=999, body="hello",
        )
        assert out is None


# ─── graphql_resolve_thread ────────────────────────────────────────────


class TestGraphqlResolveThread:
    def test_success(self, monkeypatch):
        class _FakeResult:
            stdout = ""
            returncode = 0
            stderr = ""
        captured: list[Any] = []

        def fake_run(args, **kwargs):
            captured.append(args)
            return _FakeResult()

        monkeypatch.setattr(rc.subprocess, "run", fake_run)
        assert rc.graphql_resolve_thread("T1") is True
        cmd = captured[0]
        assert "graphql" in cmd
        # Mutation must reference resolveReviewThread.
        joined = " ".join(cmd)
        assert "resolveReviewThread" in joined

    def test_failure_returns_false(self, monkeypatch, capsys):
        class _FakeResult:
            stdout = ""
            returncode = 1
            stderr = "permission denied"
        monkeypatch.setattr(
            rc.subprocess, "run",
            lambda *a, **kw: _FakeResult(),
        )
        assert rc.graphql_resolve_thread("T1") is False
        err = capsys.readouterr().err
        assert "::warning::" in err
