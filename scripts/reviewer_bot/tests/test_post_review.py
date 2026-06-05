import json

import pytest

from scripts.reviewer_bot.post_review import (
    pick_bot_inline_comment_ids,
    post_pr_review,
)


# Sample shape of a PR review comment as returned by gh api .../pulls/{n}/comments
INLINE_COMMENT_BOT = {
    "id": 100,
    "user": {"login": "peco-review-bot[bot]"},
    "body": "🔴 Critical — fix this\n<!-- pr-review-bot:v1 type=inline id=F1 -->",
}
INLINE_COMMENT_OTHER_BOT = {
    "id": 101,
    "user": {"login": "peco-review-bot[bot]"},
    "body": "different workflow's comment, no marker",
}
INLINE_COMMENT_HUMAN = {
    "id": 102,
    "user": {"login": "some-developer"},
    "body": "nit: change this name <!-- pr-review-bot:v1 type=inline id=F1 -->",
    # ↑ even with our marker, the user.login is not peco-review-bot[bot]
    # so we should NOT delete it
}


def test_pick_bot_inline_comment_ids_extracts_only_ours():
    comments = [INLINE_COMMENT_BOT, INLINE_COMMENT_OTHER_BOT, INLINE_COMMENT_HUMAN]
    ids = pick_bot_inline_comment_ids(comments)
    assert ids == [100]


def test_pick_bot_inline_comment_ids_empty_when_no_match():
    assert pick_bot_inline_comment_ids([INLINE_COMMENT_OTHER_BOT]) == []


# ── Migration-window: legacy login + marker still counts as ours ──────


INLINE_COMMENT_LEGACY_BOT = {
    "id": 103,
    "user": {"login": "github-actions[bot]"},   # legacy identity, pre-app-migration
    "body": "🟠 High — legacy comment\n<!-- pr-review-bot:v1 type=inline id=F2 -->",
}
INLINE_COMMENT_LEGACY_NO_MARKER = {
    "id": 104,
    "user": {"login": "github-actions[bot]"},
    "body": "some other workflow posted this — no marker, must NOT be cleaned up",
}
INLINE_COMMENT_HUMAN_LEGACY_MARKER_SPOOF = {
    "id": 105,
    "user": {"login": "some-developer"},
    "body": "spoofed marker — <!-- pr-review-bot:v1 type=inline id=Fevil -->",
}


def test_pick_bot_inline_comment_ids_includes_legacy_login():
    """Pre-migration: bot posted as github-actions[bot] with our marker.
    Those comments must still be picked up for reconcile after the
    app-migration lands, otherwise they'd linger forever on in-flight PRs."""
    comments = [INLINE_COMMENT_BOT, INLINE_COMMENT_LEGACY_BOT]
    ids = pick_bot_inline_comment_ids(comments)
    assert sorted(ids) == [100, 103]


def test_pick_bot_inline_comment_ids_legacy_login_without_marker_skipped():
    """github-actions[bot] is a shared identity — every other workflow
    in the repo posts under that login. We must NOT include a
    legacy-login comment that lacks our marker (it's some other
    workflow's, not ours)."""
    assert pick_bot_inline_comment_ids([INLINE_COMMENT_LEGACY_NO_MARKER]) == []


def test_pick_bot_inline_comment_ids_human_with_spoofed_marker_skipped():
    """A human author posting a body that contains our marker is NOT
    one of ours — login filter excludes them. Defense-in-depth against
    accidental or malicious marker spoofing."""
    assert pick_bot_inline_comment_ids([INLINE_COMMENT_HUMAN_LEGACY_MARKER_SPOOF]) == []


# ── post_pr_review (v2 posting model) ────────────────────────────────


class _FakeRun:
    """Records subprocess.run calls for assertion."""
    def __init__(self):
        self.calls = []
        self.stdout = ""
        self.returncode = 0
        self.stderr = ""

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        class _Result:
            pass
        r = _Result()
        r.stdout = self.stdout
        r.returncode = self.returncode
        r.stderr = self.stderr
        return r


@pytest.fixture
def fake_run(monkeypatch):
    fake = _FakeRun()
    monkeypatch.setattr("scripts.reviewer_bot.post_review.subprocess.run", fake)
    return fake


def _payload_from_call(fake_run: _FakeRun) -> dict:
    """Extract the JSON payload written to stdin of the gh api call."""
    args, kwargs = fake_run.calls[0]
    return json.loads(kwargs["input"])


def test_post_pr_review_single_inline_finding(fake_run):
    """One inline finding → POST has body + a single comments[] entry."""
    inline = [
        {"path": "src/foo.py", "line": 42, "side": "RIGHT", "body": "🔴 — fix this"},
    ]
    ok = post_pr_review(
        repo="owner/repo",
        pr_number=7,
        head_sha="abc123",
        body="Looks rough — 1 critical concern.",
        inline_findings=inline,
    )
    assert ok is True

    args, kwargs = fake_run.calls[0]
    cmd = args[0]
    # gh api -X POST .../pulls/7/reviews --input -
    assert cmd[0] == "gh"
    assert cmd[1] == "api"
    assert "-X" in cmd and "POST" in cmd
    assert "repos/owner/repo/pulls/7/reviews" in cmd
    assert "--input" in cmd and "-" in cmd

    payload = _payload_from_call(fake_run)
    assert payload["event"] == "COMMENT"
    assert payload["commit_id"] == "abc123"
    assert payload["body"] == "Looks rough — 1 critical concern."
    # GitHub's PR reviews API uses `line` + `side` (file-line + RIGHT/LEFT),
    # NOT the deprecated `position` (diff-offset). Passing a file line as
    # `position` 422s.
    assert payload["comments"] == [
        {"path": "src/foo.py", "line": 42, "side": "RIGHT", "body": "🔴 — fix this"},
    ]


def test_post_pr_review_multiple_inline_findings(fake_run):
    """Multiple inline findings → comments[] has one entry per finding,
    preserving order."""
    inline = [
        {"path": "a.py", "line": 1, "side": "RIGHT", "body": "first"},
        {"path": "b.py", "line": 2, "side": "RIGHT", "body": "second"},
        {"path": "c.py", "line": 3, "side": "RIGHT", "body": "third"},
    ]
    ok = post_pr_review(
        repo="o/r", pr_number=1, head_sha="def456",
        body="Three concerns.", inline_findings=inline,
    )
    assert ok is True

    payload = _payload_from_call(fake_run)
    assert len(payload["comments"]) == 3
    assert [c["path"] for c in payload["comments"]] == ["a.py", "b.py", "c.py"]
    assert [c["line"] for c in payload["comments"]] == [1, 2, 3]
    assert all(c["side"] == "RIGHT" for c in payload["comments"])
    assert [c["body"] for c in payload["comments"]] == ["first", "second", "third"]


def test_post_pr_review_empty_findings_still_posts(fake_run):
    """No inline findings but non-empty summary → POST still goes through
    with body + comments=[]. This is the all-clear / nit-only case."""
    ok = post_pr_review(
        repo="o/r", pr_number=2, head_sha="sha",
        body="✅ No issues identified.", inline_findings=[],
    )
    assert ok is True

    payload = _payload_from_call(fake_run)
    assert payload["body"] == "✅ No issues identified."
    assert payload["comments"] == []
    assert payload["event"] == "COMMENT"


def test_post_pr_review_returns_false_on_subprocess_failure(monkeypatch, capsys):
    """gh api non-zero exit → return False, log a ::warning::."""
    fake = _FakeRun()
    fake.returncode = 1
    fake.stderr = "HTTP 422: Validation Failed"
    monkeypatch.setattr("scripts.reviewer_bot.post_review.subprocess.run", fake)

    ok = post_pr_review(
        repo="o/r", pr_number=3, head_sha="sha",
        body="b", inline_findings=[{"path": "x", "line": 1, "side": "RIGHT", "body": "y"}],
    )
    assert ok is False

    captured = capsys.readouterr()
    assert "::warning::post_pr_review" in captured.err
    assert "pr=3" in captured.err
    assert "HTTP 422" in captured.err


def test_post_pr_review_payload_uses_stdin_not_argv(fake_run):
    """Payload MUST go through stdin (--input -) instead of as -F args.
    Many inline comments would otherwise overflow OS argv limits, AND
    JSON-encoding rules are easier to honor through stdin.
    """
    inline = [
        {"path": f"src/file{i}.py", "line": i, "side": "RIGHT", "body": f"body {i}"}
        for i in range(20)
    ]
    post_pr_review(
        repo="o/r", pr_number=4, head_sha="sha",
        body="b", inline_findings=inline,
    )
    args, kwargs = fake_run.calls[0]
    cmd = args[0]
    # No `-F body=` or `-F comments=` on argv — payload is on stdin.
    joined = " ".join(cmd)
    assert "body=" not in joined
    assert "comments=" not in joined
    assert kwargs.get("input"), "payload must be passed via stdin"
    # And the stdin payload parses as JSON and contains everything.
    payload = json.loads(kwargs["input"])
    assert len(payload["comments"]) == 20
