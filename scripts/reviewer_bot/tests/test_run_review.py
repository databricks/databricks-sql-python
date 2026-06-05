from unittest.mock import patch

import pytest

from scripts.reviewer_bot.run_review import (
    _driver_clone_url,
    _truncate_pr_body,
    build_summary_body,
    truncate_diff_to_hunk_boundary,
)
from scripts.reviewer_bot.gather_context import parse_diff_positions


def test_summary_body_includes_verdict_line():
    findings = [
        {"id": "F1", "severity": "critical", "body": "x"},
        {"id": "F2", "severity": "high", "body": "y"},
        {"id": "F3", "severity": "high", "body": "z"},
        {"id": "F4", "severity": "low", "body": "w"},
    ]
    body = build_summary_body(findings, llm_summary="LLM said: looks ok")
    assert "1 Critical" in body
    assert "2 High" in body
    assert "1 Low" in body


def test_summary_body_empty_when_no_findings():
    body = build_summary_body([], llm_summary="")
    assert "No issues identified" in body
    # Marker is added by the orchestrator wrapper, not by build_summary_body.


def test_summary_body_rejects_failed_inline_ids_kwarg():
    """F4: the v2 bundled POST is all-or-nothing, so per-finding
    failure-list rendering is dead. The parameter has been removed; if
    upstream still passes it, we want a TypeError now rather than
    silently renders dead UI."""
    import pytest
    with pytest.raises(TypeError):
        build_summary_body(
            [{"id": "F1", "severity": "high", "body": "z"}],
            llm_summary="",
            failed_inline_ids=["F1"],  # type: ignore[call-arg]
        )


def _make_diff_with_hunks(n_files: int, hunks_per_file: int) -> str:
    """Build a synthetic unified diff with predictable size."""
    parts = []
    for fi in range(n_files):
        parts.append(f"diff --git a/file{fi}.py b/file{fi}.py")
        parts.append("index abc..def 100644")
        parts.append(f"--- a/file{fi}.py")
        parts.append(f"+++ b/file{fi}.py")
        for hi in range(hunks_per_file):
            start_line = 1 + hi * 10
            parts.append(f"@@ -{start_line},2 +{start_line},3 @@")
            parts.append(f" context line for hunk {hi}")
            parts.append(f"+added line A in file {fi} hunk {hi}")
            parts.append(f"+added line B in file {fi} hunk {hi}")
    return "\n".join(parts) + "\n"


def test_truncate_diff_under_cap_returns_unchanged():
    diff = _make_diff_with_hunks(2, 1)
    out = truncate_diff_to_hunk_boundary(diff, byte_cap=10_000)
    assert out == diff


def test_truncate_diff_cuts_only_on_hunk_boundaries():
    """When the cap forces truncation, the resulting text must still be
    parseable by parse_diff_positions (no mid-hunk cut)."""
    diff = _make_diff_with_hunks(3, 5)  # 15 hunks total
    # Pick a cap that forces dropping at least some hunks.
    cap = len(diff) // 2
    out = truncate_diff_to_hunk_boundary(diff, byte_cap=cap)

    # Output advertises that hunks were dropped.
    assert "truncated:" in out
    assert "hunk(s) omitted" in out

    # parse_diff_positions consumes the truncated diff cleanly — no
    # exceptions, and every position it reports falls on a `+` line.
    positions = parse_diff_positions(out)
    assert positions  # at least some files retained

    # Sanity-check: rebuild the truncated diff into a string and confirm
    # the trailing truncation-note isn't being mis-parsed as a hunk header.
    for path, lines in positions.items():
        assert path.startswith("file")
        for line_no in lines:
            assert isinstance(line_no, int)
            assert line_no >= 1


def test_truncate_diff_preserves_first_file_when_cap_tiny():
    """If the cap is smaller than the first file's preamble, we still
    return SOMETHING coherent (the preamble + first hunk) rather than
    blowing up."""
    diff = _make_diff_with_hunks(2, 3)
    out = truncate_diff_to_hunk_boundary(diff, byte_cap=50)
    # Output begins with the file header so parse_diff_positions can
    # at least find the file.
    assert "diff --git a/file0.py" in out


def test_truncate_diff_always_includes_first_hunk_even_under_tiny_cap():
    """Regression for the Copilot finding (#317): when the cap was tinier
    than parts[0] + parts[1], the function returned ONLY parts[0] (file
    headers). That left parse_diff_positions with no hunks to map, silently
    routing every diff-anchored finding to the summary section."""
    diff = _make_diff_with_hunks(2, 5)
    # Cap small enough that the original buggy code would have returned
    # only the file headers. parts[0] for this diff is ~80-100 chars; the
    # first hunk body adds ~200 chars. A cap of 50 forces overrun even
    # for the headers, so we must accept overrun for the first hunk too.
    out = truncate_diff_to_hunk_boundary(diff, byte_cap=50)

    # The output MUST contain at least one hunk header so parse_diff_positions
    # has something to anchor against.
    assert "@@" in out, "first hunk header dropped — parse_diff_positions will see no hunks"
    # parse_diff_positions must succeed and return at least one mapped position
    positions = parse_diff_positions(out)
    assert positions, "no diff positions mapped — all findings would route to summary"


def test_truncate_diff_no_hunks_falls_back_to_raw_cap():
    """A diff-shaped text with no hunk headers falls back to a raw
    byte cap — this branch is defensive, real diffs always have hunks."""
    blob = "diff --git a/f b/f\n--- a/f\n+++ b/f\n" + "x" * 1000
    out = truncate_diff_to_hunk_boundary(blob, byte_cap=100)
    assert len(out) == 100


# ── _driver_clone_url tests ───────────────────────────────────────


def test_driver_clone_url_is_plain_https():
    """SECURITY regression: the URL must NEVER contain a token. It used to
    be embedded as `https://x-access-token:<TOKEN>@github.com/...` which
    leaked into process argv and `.git/config`'s `origin` URL. The function
    is now a constant — auth is passed via a git `http.<url>.extraheader`
    config flag at clone time (tested separately at the call site).

    This test exists to catch a future regression that re-introduces
    token embedding here. Stays even though the function is now trivial."""
    url = _driver_clone_url()
    assert url == "https://github.com/adbc-drivers/databricks.git"
    # Strong negative: no `@` in the host portion (no `user:password@host` form).
    assert "@" not in url
    assert "x-access-token" not in url


# ── _truncate_pr_body tests ───────────────────────────────────────


def test_truncate_pr_body_short_passes_through():
    """Body under the cap is returned verbatim — no marker injected."""
    body = "short pr description"
    assert _truncate_pr_body(body) == body
    assert "[... truncated" not in _truncate_pr_body(body)


def test_truncate_pr_body_long_includes_marker():
    """Body over the cap is truncated and gets a visible marker telling
    the model how many chars were clipped, so it can reason about the
    missing context."""
    body = "x" * 5000
    out = _truncate_pr_body(body, cap=4000)
    assert out.startswith("x" * 4000)
    assert "[... truncated, 1000 more chars ...]" in out
    # The marker is APPENDED — the original prefix must still be intact.
    assert out[:4000] == body[:4000]


def test_truncate_pr_body_at_exact_cap_no_marker():
    """Body exactly at the cap is not truncated."""
    body = "x" * 4000
    out = _truncate_pr_body(body, cap=4000)
    assert out == body
    assert "truncated" not in out


# ── build_summary_body unknown-severity verdict ───────────────────


def test_summary_body_unknown_severity_does_not_say_no_findings():
    """If findings exist but none of their severities match the known
    buckets, the verdict must reflect that findings exist — saying
    "No findings" while listing findings below is misleading."""
    findings = [
        {"id": "F1", "severity": "unknown", "body": "weird"},
        {"id": "F2", "severity": "bogus", "body": "also weird"},
    ]
    body = build_summary_body(findings, llm_summary="")
    # No known severities → verdict falls back to the count
    assert "2 findings" in body
    # And must NOT claim there are no findings
    assert "No findings" not in body


def test_summary_body_no_findings_still_says_no_findings():
    """The truly-empty case (no findings) keeps the cheerful all-clear
    verdict."""
    body = build_summary_body([], llm_summary="")
    assert "No issues identified" in body


# ── _post_failure & _run_gh pagination flag tests ─────────────────


def test_second_stale_sha_guard_catches_broad_exceptions():
    """The second stale-SHA guard (immediately before posting) must catch
    broader exceptions than just CalledProcessError. If `gh` is missing
    from PATH (FileNotFoundError), or any OSError fires during the
    `gh pr view` call, an uncaught exception would bubble out of main()
    and bypass _post_failure entirely — the PR would see no signal.

    Source-inspection (cheap; matches the test style for the other
    stale-SHA guard below)."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "run_review.py"
    text = src.read_text()
    # The second stale-SHA guard must use `except Exception` to catch
    # not just CalledProcessError but FileNotFoundError (missing gh),
    # OSError (network/io), etc.
    # The second stale-SHA guard's except clause uses `except Exception`.
    # Locate the guard by its warning message, then verify there's an
    # `except Exception` clause earlier in the same try block (not a
    # narrow `except subprocess.CalledProcessError`).
    guard_idx = text.find("stale-SHA guard before post failed")
    assert guard_idx > 0, "could not locate the second stale-SHA guard"
    # Look back at most ~2000 chars for the except clause of the
    # surrounding try (covers the docstring + comment block above).
    preceding = text[max(0, guard_idx - 2000):guard_idx]
    assert "except Exception" in preceding, (
        "the second stale-SHA guard's except clause must be widened to "
        "Exception so a missing-gh / network / OS error doesn't bubble out "
        "of main() and bypass _post_failure."
    )
    # And it must NOT be the narrow CalledProcessError-only form.
    # (Search the small window just before the warning, not the whole file
    # — there's a legitimate narrow-catch elsewhere.)
    near = text[max(0, guard_idx - 300):guard_idx]
    assert "except subprocess.CalledProcessError" not in near, (
        "second stale-SHA guard still uses narrow CalledProcessError catch"
    )


def test_stale_sha_guard_compares_against_env_head_sha():
    """The first stale-SHA guard must compare the FRESH headRefOid
    against the trigger-time HEAD_SHA env var, NOT against
    pr_meta['headRefOid'] from the same `gh pr view` call (which would
    be a tautology — they came from the same network call so they're
    always equal).

    Encoded as a source-inspection test for the same reasons as the
    --paginate test: cheap, fast, and immune to spurious changes that
    don't touch the guard itself."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "run_review.py"
    text = src.read_text()
    # The guard should reference the env-derived head_sha local, not
    # `pr_meta['headRefOid'] != pr_meta['headRefOid']` or any other
    # self-comparison.
    assert "pr_meta[\"headRefOid\"] != head_sha" in text, (
        "stale-SHA guard at the start of Phase 2 must compare against "
        "the trigger-time `head_sha` env var, not against itself."
    )
    # Defensive: no `pr_meta["headRefOid"] != pr_meta["headRefOid"]`.
    assert "pr_meta[\"headRefOid\"] != pr_meta[\"headRefOid\"]" not in text


def test_phase_2_clone_failure_calls_post_failure(monkeypatch, tmp_path):
    """A failing driver clone must route through _post_failure (so the
    PR sees a posted review) instead of crashing main() before the
    failure-handler is in scope."""
    import json
    import subprocess

    from scripts.reviewer_bot import run_review

    # Stub the env so main() doesn't bail early.
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    monkeypatch.setenv("PR_NUMBER", "1")
    monkeypatch.setenv("HEAD_SHA", "a" * 40)
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    monkeypatch.setenv("MODEL_ENDPOINT", "http://endpoint")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv("DRY_RUN", "false")
    # Force the failure-handler path.

    pr_view_payload = json.dumps({
        "title": "t",
        "body": "b",
        "url": "u",
        "headRefOid": "a" * 40,  # matches HEAD_SHA — no stale-SHA skip
        "baseRefOid": "b" * 40,
        "files": [{"path": "tests/csharp/Foo.cs"}],
    })

    def fake_run_gh(args):
        # `pr view` returns metadata; `pr diff` returns a diff string.
        joined = " ".join(args)
        if "pr" in args and "view" in args:
            return pr_view_payload
        if "pr" in args and "diff" in args:
            return ""  # empty diff is fine
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    # Force the git clone subprocess to fail.
    def fake_subprocess_run(*args, **kwargs):
        cmd = args[0]
        if cmd and cmd[0] == "git" and "clone" in cmd:
            raise subprocess.CalledProcessError(
                returncode=128, cmd=cmd,
                output="", stderr="fatal: could not read Username for 'https://github.com'",
            )
        # Any other subprocess (post failure summary, etc.) — succeed.
        class _R:
            returncode = 0
            stdout = ""
            stderr = ""
        return _R()

    monkeypatch.setattr(run_review.subprocess, "run", fake_subprocess_run)

    posted: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        posted.append({"repo": repo, "pr_number": pr_number, "body": body,
                       "inline_findings": inline_findings})
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    # main() returns 1 (failure) via _post_failure, NOT raises.
    assert rc == 1
    # _post_failure was reached and called post_pr_review.
    assert posted, "Driver-clone failure must route through _post_failure"
    assert "failed" in posted[0]["body"].lower()
    # Failure reviews carry no inline findings.
    assert posted[0]["inline_findings"] == []


def test_phase_2_pr_view_failure_calls_post_failure(monkeypatch, tmp_path):
    """`gh pr view` failure (e.g., transient API error) must route through
    _post_failure too, not crash."""
    import subprocess

    from scripts.reviewer_bot import run_review

    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    monkeypatch.setenv("PR_NUMBER", "1")
    monkeypatch.setenv("HEAD_SHA", "a" * 40)
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    monkeypatch.setenv("MODEL_ENDPOINT", "http://endpoint")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv("DRY_RUN", "false")

    call_count = {"n": 0}

    def fake_run_gh(args):
        call_count["n"] += 1
        # Fail the first call (the `gh pr view` for metadata).
        if call_count["n"] == 1:
            raise subprocess.CalledProcessError(
                returncode=1, cmd=args,
                output="", stderr="HTTP 502 bad gateway",
            )
        # Subsequent calls (the failure handler's listing) succeed.
        return "[]"

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    posted: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        posted.append({"body": body})
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 1
    assert posted, "Phase 2 gh-api failure must route through _post_failure"
    assert "phase 2" in posted[0]["body"].lower() or "failed" in posted[0]["body"].lower()


# ── Crash-bug regression: non-dict findings ──────────────────────


def _setup_main_env(monkeypatch, tmp_path):
    """Common env setup for end-to-end `main()` tests."""
    monkeypatch.setenv("GITHUB_REPOSITORY", "o/r")
    monkeypatch.setenv("PR_NUMBER", "1")
    monkeypatch.setenv("HEAD_SHA", "a" * 40)
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    monkeypatch.setenv("MODEL_ENDPOINT", "http://endpoint")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv("DRY_RUN", "true")  # default to dry-run for safety


def test_main_filters_non_dict_findings(monkeypatch, tmp_path, capsys):
    """REGRESSION: the LLM occasionally returns a `findings` list where
    one or more entries are bare strings (or otherwise non-dict). The
    pre_demote dict-comp used to crash on `f.get(...)`. Filter at the
    boundary and log a warning."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40  # second stale-SHA check: current SHA
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    # run_agent returns a mix of dicts and a stray string.
    def fake_run_agent(**_kwargs):
        return {
            "findings": [
                {"id": "F1", "severity": "high", "body": "real one",
                 "citation": "c", "file": "src/foo.py", "line": 1},
                "this is not a dict",   # the offending entry
                {"id": "F2", "severity": "low", "body": "another"},
            ],
            "summary": "",
        }

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)

    rc = run_review.main()
    # No crash → rc == 0 (dry-run path).
    assert rc == 0
    # The warning must surface in the observer log output (mirrored to
    # stderr / step summary), confirming the drop was visible.
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "Dropped 1 non-dict" in combined


# ── Low/Nit inline → summary re-route ────────────────────────────


def test_low_findings_on_diff_lines_route_inline(monkeypatch, tmp_path, capsys):
    """v2 posting model: a Low finding landing on a diff-anchored line
    must route to `inline` and end up in the bundled review's
    comments[] array. Previously (v1) Lows were summary-only; v2
    promotes Low into INLINE_ELIGIBLE because a file:line citation is
    more useful than a buried bullet."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)

    # Diff that adds a single line to src/foo.py at line 1.
    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40  # second stale-SHA check: current SHA
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    def fake_run_agent(**_kwargs):
        return {
            "findings": [
                {
                    "id": "L1", "severity": "low",
                    "body": "minor nit on the new line",
                    "citation": "src/foo.py:2",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": "",
        }

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)

    rc = run_review.main()
    assert rc == 0
    out = capsys.readouterr().out
    # The dry-run print includes the JSON findings dump — Low must
    # now route inline (NOT summary as in v1).
    assert '"_route": "inline"' in out, (
        "Low finding on diff-anchored line must route inline under the "
        "v2 INLINE_ELIGIBLE set (Critical/High/Medium/Low)."
    )
    # And the bundled inline-comments payload should contain it.
    assert "minor nit on the new line" in out


def test_nit_findings_on_diff_lines_still_route_to_summary(
    monkeypatch, tmp_path, capsys,
):
    """v2 contract: Nit findings still route to summary (the review body),
    not inline. Anchor doesn't add value for preference / formatting
    findings."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    def fake_run_agent(**_kwargs):
        return {
            "findings": [
                {
                    "id": "N1", "severity": "nit",
                    "body": "spelling preference",
                    "citation": "CLAUDE.md:1",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": "",
        }

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)

    rc = run_review.main()
    assert rc == 0
    out = capsys.readouterr().out
    # Nit on a diff-anchored line MUST be re-routed to summary so it
    # appears in the review body's "Other findings" section, not in
    # the inline comments array.
    assert '"_route": "summary"' in out, (
        "Nit finding on diff-anchored line must be re-routed to summary "
        "— inline anchor doesn't add value for preference findings."
    )
    assert "Other findings" in out
    assert "spelling preference" in out


# ── Second stale-SHA guard tolerates transient gh failure ─────────


def test_second_stale_sha_guard_tolerates_transient_failure(
    monkeypatch, tmp_path, capsys,
):
    """REGRESSION (PR #317 review comment 3283514089): if the SECOND
    `gh pr view` (the pre-post stale-SHA check) fails transiently, the
    LLM run must still post — we already paid for it."""
    import json
    import subprocess

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")  # exercise the real post path

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    call_count = {"n": 0}

    def fake_run_gh(args):
        call_count["n"] += 1
        # First call: the Phase-2 `gh pr view` succeeds.
        if "view" in args and "headRefOid" in args and "--jq" in args:
            # This is the SECOND stale-SHA check (uses --jq).
            raise subprocess.CalledProcessError(
                returncode=1, cmd=args,
                output="", stderr="HTTP 502 transient",
            )
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        if "comments" in " ".join(args):
            return "[]"
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    def fake_run_agent(**_kwargs):
        return {"findings": [], "summary": "ok"}

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)

    # Capture whether post_pr_review was called (the proof that
    # we proceeded past the failed stale-SHA check).
    posted: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        posted.append({"body": body, "inline_findings": inline_findings})
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 0
    assert posted, (
        "Review must still post even when the second stale-SHA guard's "
        "`gh pr view` call raises — the LLM result is already paid for."
    )
    # And the warning should have been emitted to stderr.
    err = capsys.readouterr().err
    assert "stale-SHA guard before post failed" in err


# ── Driver clone never leaks the token via argv ──────────────────


def test_driver_clone_passes_token_via_extraheader_not_url(
    monkeypatch, tmp_path,
):
    """SECURITY (PR #317 review comment 3283636851): the integration
    test app token MUST NOT appear in the clone URL or anywhere else in
    the `git clone` argv. It must be passed via an `http.<url>.extraheader`
    config flag so it lives only in the HTTPS request header for the
    one clone operation."""
    import json
    import subprocess

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    secret = "ghs_super_secret_token_must_never_leak"
    monkeypatch.setenv("INTEGRATION_TEST_APP_TOKEN", secret)

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        # Force the driver-clone branch via tests/csharp/.
        "files": [{"path": "tests/csharp/Foo.cs"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    captured_argv: list[list[str]] = []

    def fake_subprocess_run(*args, **kwargs):
        cmd = list(args[0]) if args else []
        captured_argv.append(cmd)
        class _R:
            returncode = 0
            stdout = ""
            stderr = ""
        return _R()

    monkeypatch.setattr(run_review.subprocess, "run", fake_subprocess_run)

    def fake_run_agent(**_kwargs):
        return {"findings": [], "summary": "ok"}

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)

    rc = run_review.main()
    assert rc == 0

    # Find the clone subprocess invocation.
    clone_calls = [c for c in captured_argv if "clone" in c]
    assert clone_calls, "Expected git clone to be invoked"
    clone_argv = clone_calls[0]

    # 1. The token MUST NOT appear in any positional URL arg.
    for arg in clone_argv:
        if arg.startswith("https://") and "github.com/adbc-drivers/databricks" in arg:
            assert secret not in arg, (
                f"Token leaked into clone URL arg: {arg!r}"
            )

    # 2. The token MAY appear inside an extraheader config value, but
    # MUST NOT appear as a standalone arg or in the URL.
    # Find the extraheader arg.
    extraheader_args = [a for a in clone_argv if "extraheader" in a]
    assert extraheader_args, (
        "Expected `-c http.<url>.extraheader=AUTHORIZATION: bearer <token>` "
        "to be passed when INTEGRATION_TEST_APP_TOKEN is set."
    )
    # The token should be inside the auth header value.
    assert any(secret in a and "extraheader" in a for a in extraheader_args), (
        "Token must be inside the extraheader value, not anywhere else."
    )

    # 3. credential.helper= (empty) should be set to disable system helpers.
    assert any("credential.helper=" in a for a in clone_argv), (
        "Expected `-c credential.helper=` (empty) to disable system helpers."
    )

    # 4. Post-clone, a `remote set-url` should run to defensively reset
    # the remote to the token-free URL.
    set_url_calls = [c for c in captured_argv if "set-url" in c and "remote" in c]
    assert set_url_calls, (
        "Expected a defensive `git remote set-url origin <url>` after clone."
    )
    set_url_argv = set_url_calls[0]
    # And that URL must not contain the token.
    for arg in set_url_argv:
        assert secret not in arg, (
            f"Token leaked into post-clone remote set-url: {arg!r}"
        )


# ── v2 posting model: main() calls post_pr_review once per run ──


def test_main_calls_post_pr_review_with_body_and_inline(
    monkeypatch, tmp_path,
):
    """v2 contract: main() makes a single `post_pr_review` call per push
    with the review body + every inline finding bundled into the
    inline_findings list. No sticky-comment posting / patching /
    deletion happens any more."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    # Diff with an added line at line 2 so the High finding lands
    # on a diff-anchored position and routes inline.
    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {
            "findings": [
                {
                    # Medium (not High) so the blocking-findings exit gate
                    # doesn't fire — this test is about post_pr_review's
                    # call shape, not the gate.
                    "id": "F1", "severity": "medium",
                    "body": "real concern",
                    "citation": "src/foo.py:2",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": "Looks rough.",
        },
    )

    captured: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        captured.append({
            "repo": repo, "pr_number": pr_number, "head_sha": head_sha,
            "body": body, "inline_findings": list(inline_findings),
        })
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 0
    # Single call per run — never multiple.
    assert len(captured) == 1
    call = captured[0]
    assert call["pr_number"] == 1
    assert call["head_sha"] == "a" * 40
    # Body contains the verdict + LLM summary.
    assert "1 Medium" in call["body"]
    # The Medium finding shows up as a single inline-findings entry.
    assert len(call["inline_findings"]) == 1
    entry = call["inline_findings"][0]
    assert entry["path"] == "src/foo.py"
    assert entry["line"] == 2
    assert entry["side"] == "RIGHT"
    assert "real concern" in entry["body"]
    # Marker must be embedded in the inline body so reconcile can id it.
    assert "pr-review-bot:v1 type=inline" in entry["body"]


def test_main_records_failure_when_review_post_fails(monkeypatch, tmp_path):
    """When `post_pr_review` returns False, `main()` must call
    `observer.record_failure` so the workflow step-summary shows the
    failed post instead of reporting an all-clear."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {"findings": [], "summary": "ok"},
    )

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        lambda **_kw: False,
    )

    rc = run_review.main()
    # We don't fail the run, but the observer records it.
    assert rc == 0


def test_main_logs_review_failure_to_observer(monkeypatch, tmp_path, capsys):
    """Companion to the above: confirms the failure is *visibly* logged
    so a reviewer scanning the workflow output sees it."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {"findings": [], "summary": "ok"},
    )
    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        lambda **_kw: False,
    )

    run_review.main()
    combined = capsys.readouterr().out + capsys.readouterr().err
    # The "::error::" prefix surfaces in GitHub Actions logs as an error
    # annotation, making the missed review impossible to overlook.
    assert "PR review post failed" in combined or \
        "pr review post failed" in combined.lower()


def test_main_does_not_delete_prior_bot_comments(monkeypatch, tmp_path):
    """v2 posting model: prior bot inline comments are NEVER deleted.
    Each review is a discrete timeline event; reconcile (Phase 6)
    handles old-thread cleanup naturally without removing comments."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    gh_calls: list[list[str]] = []

    def fake_run_gh(args):
        gh_calls.append(list(args))
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {"findings": [], "summary": "ok"},
    )
    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        lambda **_kw: True,
    )

    rc = run_review.main()
    assert rc == 0

    # NO DELETE calls anywhere — the v2 model never pre-cleans bot
    # comments. (Reconcile does GraphQL resolves, not REST DELETEs.)
    for args in gh_calls:
        joined = " ".join(args)
        assert "DELETE" not in args, (
            f"v2 model must not issue DELETE calls, but saw: {joined}"
        )
    # And no issues-comments listing either — the sticky model is gone.
    for args in gh_calls:
        joined = " ".join(args)
        assert "issues/" not in joined or "/comments" not in joined, (
            f"v2 model must not list issues/comments (sticky gone): {joined}"
        )



# ── F1: _post_failure must not pass an empty commit_id ───────────


def test_post_failure_falls_back_to_gh_pr_view_when_head_sha_empty(
    monkeypatch, capsys,
):
    """F1 (High): _post_failure used to call post_pr_review(head_sha="")
    when HEAD_SHA was unset, which 422s the bundled
    POST /pulls/{n}/reviews. Now: fall back to fetching the current
    head via `gh pr view --jq .headRefOid`."""
    from scripts.reviewer_bot import run_review

    # HEAD_SHA explicitly absent — exercises the fallback branch.
    monkeypatch.delenv("HEAD_SHA", raising=False)

    fetched_sha = "f" * 40
    gh_calls: list[list[str]] = []

    def fake_run_gh(args):
        gh_calls.append(list(args))
        # The fallback should request headRefOid via --jq.
        if "headRefOid" in args and "--jq" in args:
            return fetched_sha + "\n"
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    posted: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        posted.append({
            "head_sha": head_sha, "body": body,
            "inline_findings": inline_findings,
        })
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review._post_failure(
        repo="o/r", pr_number=42, reason="boom", dry_run=False,
    )
    assert rc == 1

    # (a) The fallback gh call WAS made.
    assert any(
        "headRefOid" in args and "--jq" in args for args in gh_calls
    ), "Expected `gh pr view --jq .headRefOid` fallback to be invoked"

    # (b) post_pr_review was called with the FETCHED sha (not "").
    assert posted, "post_pr_review should be called after successful fallback"
    assert posted[0]["head_sha"] == fetched_sha, (
        "post_pr_review must receive the fetched SHA from the fallback, "
        "not the empty env var."
    )
    assert posted[0]["inline_findings"] == []


def test_post_failure_skips_post_when_fallback_also_fails(
    monkeypatch, capsys,
):
    """F1: if HEAD_SHA is empty AND the gh fallback fails, log
    ::error:: and return 1 WITHOUT calling post_pr_review (an empty
    commit_id would just 422 anyway)."""
    import subprocess

    from scripts.reviewer_bot import run_review

    monkeypatch.delenv("HEAD_SHA", raising=False)

    def fake_run_gh(args):
        raise subprocess.CalledProcessError(
            returncode=1, cmd=args,
            output="", stderr="HTTP 502",
        )

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    posted: list[dict] = []

    def fake_post_pr_review(**kwargs):
        posted.append(kwargs)
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review._post_failure(
        repo="o/r", pr_number=42, reason="boom", dry_run=False,
    )
    assert rc == 1
    assert not posted, (
        "post_pr_review must NOT be called with an empty SHA — the "
        "POST would 422 anyway, and the workflow's exit code already "
        "reflects the failure."
    )
    err = capsys.readouterr().err
    assert "::error::" in err
    assert "no HEAD_SHA available" in err


def test_post_failure_uses_env_head_sha_when_present(monkeypatch):
    """Sanity: when HEAD_SHA is set, _post_failure passes it through
    WITHOUT calling the fallback. Locks in that the F1 fix is gated on
    the empty case, not always-on."""
    from scripts.reviewer_bot import run_review

    monkeypatch.setenv("HEAD_SHA", "a" * 40)

    gh_calls: list[list[str]] = []

    def fake_run_gh(args):
        gh_calls.append(list(args))
        return "wrong" * 8  # would override if the fallback ran

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    posted: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        posted.append({"head_sha": head_sha})
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review._post_failure(
        repo="o/r", pr_number=42, reason="boom", dry_run=False,
    )
    assert rc == 1
    # Fallback gh call NOT made.
    assert not gh_calls, "Fallback gh call must not run when HEAD_SHA is set"
    # And the env SHA was used verbatim.
    assert posted[0]["head_sha"] == "a" * 40


# ── F2: proactive drop + reactive body-only retry ───────────────


def test_main_drops_inline_finding_not_in_diff_positions(
    monkeypatch, tmp_path, capsys,
):
    """F2 proactive: an inline-routed finding whose (path, line) is
    NOT in diff_positions must be re-routed to summary BEFORE the
    bundled POST — otherwise GitHub 422s the whole review.

    We construct a scenario where route_by_diff_position would set
    _route=inline (because the finding has a file:line and we're
    testing the F2 belt-and-braces re-check), but the line isn't in
    diff_positions (because we lie about the line in the model output).

    Easiest way to exercise: monkeypatch route_by_diff_position to
    force _route=inline so the proactive validation gets the
    hallucinated finding."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    # Diff anchors src/foo.py:2 only.
    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    def fake_run_agent(**_kwargs):
        # Hallucinated line 999 — not in diff_positions (only line 2 is).
        # Use medium severity so the blocking-findings exit gate doesn't
        # interfere with this test's focus on F2 hallucination handling.
        return {
            "findings": [
                {
                    "id": "F_HALLUC", "severity": "medium",
                    "body": "hallucinated concern",
                    "citation": "src/foo.py:999",
                    "file": "src/foo.py", "line": 999,
                },
                # And a real one that SHOULD post inline.
                {
                    "id": "F_REAL", "severity": "medium",
                    "body": "real concern",
                    "citation": "src/foo.py:2",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": "ok",
        }

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)

    # Force the hallucinated finding to route inline so we exercise the
    # F2 belt-and-braces re-check. route_by_diff_position would normally
    # route it to summary because line 999 isn't in diff_positions; we
    # bypass that here to simulate a router bug or a future change that
    # could send an unanchorable finding past the routing stage.
    original_route = run_review.vf.route_by_diff_position

    def fake_route(findings, diff_positions):
        out = original_route(findings, diff_positions)
        for f in out:
            if f.get("id") == "F_HALLUC":
                f["_route"] = "inline"
        return out

    monkeypatch.setattr(run_review.vf, "route_by_diff_position", fake_route)

    captured: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        captured.append({
            "body": body, "inline_findings": list(inline_findings),
        })
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 0

    assert len(captured) == 1
    call = captured[0]
    # Only the REAL finding made it into inline_findings.
    assert len(call["inline_findings"]) == 1
    assert call["inline_findings"][0]["line"] == 2
    assert "real concern" in call["inline_findings"][0]["body"]
    # The hallucinated one was re-routed to summary → appears in body's
    # "Other findings" section.
    assert "Other findings" in call["body"]
    assert "hallucinated concern" in call["body"]
    # And a warning was emitted for the dropped entry.
    err = capsys.readouterr().err
    assert "F_HALLUC" in err
    assert "not in diff_positions" in err
    assert "re-routing to summary" in err


def test_main_retries_body_only_when_post_with_inline_fails(
    monkeypatch, tmp_path, capsys,
):
    """F2 reactive: if post_pr_review(...) returns False AND there were
    inline findings, retry once with comments=[] so at least the verdict
    body lands. The verdict + summary findings reaching the PR is far
    more useful than total silence."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {
            "findings": [
                {
                    "id": "F1", "severity": "medium",
                    "body": "real concern",
                    "citation": "src/foo.py:2",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": "ok",
        },
    )

    calls: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        calls.append({
            "body": body, "inline_findings": list(inline_findings),
        })
        # First call (with inline_findings) fails; second call
        # (body-only) succeeds.
        return len(calls) == 2

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 0

    # Exactly two calls: first with inline, second body-only.
    assert len(calls) == 2
    assert len(calls[0]["inline_findings"]) == 1, (
        "First post attempt should include the inline payload"
    )
    assert calls[1]["inline_findings"] == [], (
        "Retry attempt must drop the comments[] array (body-only post)"
    )
    # The retry body MUST include the originally-inline finding's body
    # in its "Other findings" section — otherwise the finding vanishes
    # from the PR entirely (only the verdict count would mention it).
    assert "real concern" in calls[1]["body"], (
        "Retry body must include the re-routed inline finding's body"
    )
    assert "Other findings" in calls[1]["body"], (
        "Retry body must contain the 'Other findings' section"
    )

    err = capsys.readouterr().err
    assert "review post failed with inline comments" in err
    assert "re-routing inline findings to summary" in err
    assert "re-routed 1 inline finding(s) to summary" in err


def test_main_no_retry_when_post_succeeds_first_try(
    monkeypatch, tmp_path, capsys,
):
    """F2: the body-only retry must NOT fire when the first post
    succeeds. Locks in that the retry is gated on (failure AND inline
    non-empty), not always-on."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {
            "findings": [
                {
                    "id": "F1", "severity": "medium",
                    "body": "real concern",
                    "citation": "src/foo.py:2",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": "ok",
        },
    )

    calls: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        calls.append({"inline_findings": list(inline_findings)})
        return True  # first attempt succeeds

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 0
    assert len(calls) == 1, "Body-only retry must not fire on first-try success"
    err = capsys.readouterr().err
    assert "falling back to body-only post" not in err


def test_main_no_retry_when_first_post_failed_with_no_inline(
    monkeypatch, tmp_path, capsys,
):
    """F2: if the first post fails AND there were no inline findings,
    the retry is pointless (a body-only retry would re-issue the same
    failing call). The hard-failure path stands."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return ""
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {"findings": [], "summary": "ok"},
    )

    calls: list[dict] = []

    def fake_post_pr_review(*, repo, pr_number, head_sha, body, inline_findings, event="COMMENT"):
        calls.append({"inline_findings": list(inline_findings)})
        return False  # always fail

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )

    rc = run_review.main()
    assert rc == 0
    # Only the original call — no body-only retry since inline was empty.
    assert len(calls) == 1
    err = capsys.readouterr().err
    assert "falling back to body-only post" not in err


# ── Blocking findings exit gate ──────────────────────────────────


@pytest.mark.parametrize("severity", [
    # Lowercase (the canonical form)
    "critical", "high",
    # Uppercase / mixed-case — must also block since the gate is
    # case-insensitive (matching the APPROVE gate).
    "Critical", "HIGH", "High",
])
def test_main_exits_non_zero_when_blocking_findings(
    monkeypatch, tmp_path, capsys, severity,
):
    """Critical OR High findings → main() returns 1 so branch protection
    can gate merges on the bot's check. Medium/Low/Nit DO NOT block.

    Parameterized to cover BOTH severities — the gate's docstring claims
    "Critical/High", so both must exercise the same exit path."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )
    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        if "comments" in " ".join(args):
            return "[]"
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    def fake_run_agent(**_kwargs):
        return {
            "findings": [
                {
                    "id": "B1", "severity": severity,
                    "body": f"{severity} concern",
                    "citation": "src/foo.py:2",
                    "file": "src/foo.py", "line": 2,
                },
            ],
            "summary": f"{severity.title()} concern.",
        }

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)
    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        lambda **_kw: True,
    )

    rc = run_review.main()
    assert rc == 1, f"{severity} finding must cause main() to return non-zero"

    captured = capsys.readouterr()
    # ::error:: annotation MUST land on stdout (GitHub Actions parses
    # workflow commands from stdout; stderr leaves the line in the raw
    # log but no red annotation renders).
    assert "::error::Bot found 1 blocking finding(s)" in captured.out
    assert "Critical/High" in captured.out


def test_main_exits_zero_on_medium_low_nit_findings_only(monkeypatch, tmp_path):
    """Medium / Low / Nit findings — even many of them — do NOT block
    the merge. The bot is advisory at these levels."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )
    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        if "comments" in " ".join(args):
            return "[]"
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)

    def fake_run_agent(**_kwargs):
        return {
            "findings": [
                {"id": "M1", "severity": "medium", "body": "m", "file": "src/foo.py", "line": 2},
                {"id": "L1", "severity": "low", "body": "l", "file": "src/foo.py", "line": 2},
                {"id": "N1", "severity": "nit", "body": "n", "file": "src/foo.py", "line": 2},
            ],
            "summary": "Advisory; nothing blocking.",
        }

    monkeypatch.setattr(run_review, "run_agent", fake_run_agent)
    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        lambda **_kw: True,
    )

    rc = run_review.main()
    assert rc == 0, "Medium/Low/Nit findings must NOT block merge"


def test_main_records_failure_on_blocking_findings(monkeypatch, tmp_path):
    """When the bot returns 1 due to blocking findings, the Observer must
    record the failure BEFORE write_step_summary is called — otherwise
    the step summary shows "Posted review" status alongside a red
    workflow check, which is confusing operator UX."""
    import json

    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )
    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        if "comments" in " ".join(args):
            return "[]"
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {
            "findings": [
                {"id": "C1", "severity": "critical", "body": "x",
                 "citation": "src/foo.py:2",   # avoid demote-to-nit
                 "file": "src/foo.py", "line": 2},
            ],
            "summary": "Critical issue.",
        },
    )

    # Capture record_failure calls.
    record_failure_calls = []
    real_record_failure = run_review.Observer.record_failure

    def capture_record_failure(self, reason):
        record_failure_calls.append(reason)
        return real_record_failure(self, reason)

    monkeypatch.setattr(run_review.Observer, "record_failure", capture_record_failure)
    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        lambda **_kw: True,
    )

    rc = run_review.main()
    assert rc == 1, "Critical finding must cause main() to exit 1"
    # record_failure WAS called (so the step summary's Outcome row shows
    # ⚠️ FAILED instead of "Posted review").
    assert any("blocking finding" in r for r in record_failure_calls), (
        f"Expected Observer.record_failure call mentioning 'blocking finding'; got: {record_failure_calls}"
    )


# ── APPROVE-when-clean: bot submits APPROVE when no Medium+ findings ──


def _run_main_with_findings(monkeypatch, tmp_path, findings, post_capture):
    """Helper: run main() with the given findings list + capture
    post_pr_review's event arg. Returns the rc."""
    import json
    from scripts.reviewer_bot import run_review

    _setup_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("DRY_RUN", "false")

    diff = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "index abc..def 100644\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
        "@@ -1,1 +1,2 @@\n"
        " context\n"
        "+added line\n"
    )
    pr_view_payload = json.dumps({
        "title": "t", "body": "b", "url": "u",
        "headRefOid": "a" * 40, "baseRefOid": "b" * 40,
        "files": [{"path": "src/foo.py"}],
    })

    def fake_run_gh(args):
        if "view" in args and "--jq" in args:
            return "a" * 40
        if "view" in args:
            return pr_view_payload
        if "diff" in args:
            return diff
        if "comments" in " ".join(args):
            return "[]"
        return ""

    monkeypatch.setattr(run_review, "_run_gh", fake_run_gh)
    monkeypatch.setattr(
        run_review, "run_agent",
        lambda **_kw: {"findings": findings, "summary": "ok"},
    )

    def fake_post_pr_review(**kwargs):
        post_capture.append(kwargs)
        return True

    monkeypatch.setattr(
        "scripts.reviewer_bot.post_review.post_pr_review",
        fake_post_pr_review,
    )
    return run_review.main()


def test_main_approves_when_no_findings(monkeypatch, tmp_path):
    """Empty findings list → bot submits event=APPROVE."""
    posts = []
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings=[], post_capture=posts)
    assert rc == 0
    assert posts[0]["event"] == "APPROVE", (
        f"Empty findings should yield APPROVE; got {posts[0]['event']}"
    )


def test_main_approves_when_only_low_and_nit(monkeypatch, tmp_path):
    """Only Low and Nit findings → APPROVE. Low/Nit are advisory only."""
    posts = []
    findings = [
        {"id": "L1", "severity": "low", "body": "minor",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
        {"id": "N1", "severity": "nit", "body": "style",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
    ]
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings, posts)
    assert rc == 0
    assert posts[0]["event"] == "APPROVE", (
        f"Only Low/Nit should yield APPROVE; got {posts[0]['event']}"
    )


@pytest.mark.parametrize("severity", ["medium", "high", "critical"])
def test_main_does_not_approve_when_medium_or_higher_present(
    monkeypatch, tmp_path, severity,
):
    """Any Medium / High / Critical finding → event=COMMENT (no APPROVE).
    The bot withholds approval when it sees real concerns."""
    posts = []
    findings = [
        {"id": "X1", "severity": severity, "body": "concern",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
    ]
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings, posts)
    # Critical/High still return 1 (blocking gate); Medium returns 0.
    assert posts[0]["event"] == "COMMENT", (
        f"{severity} severity must withhold APPROVE; got {posts[0]['event']}"
    )


def test_main_approves_with_inline_low_findings(monkeypatch, tmp_path):
    """A Low finding can be inline-eligible AND the bot still APPROVES —
    inline-eligibility doesn't change the approval decision."""
    posts = []
    findings = [
        {"id": "L1", "severity": "low", "body": "minor",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
    ]
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings, posts)
    assert rc == 0
    assert posts[0]["event"] == "APPROVE"
    # Low with diff anchor IS inline — gets included in comments[].
    assert len(posts[0]["inline_findings"]) == 1
    assert posts[0]["inline_findings"][0]["body"].startswith("🔵")


def test_main_does_not_approve_on_unknown_severity(monkeypatch, tmp_path, capsys):
    """SECURITY/SAFETY: a finding with an unrecognized severity (typo,
    missing field, new severity added later) must NOT silently fall
    through to APPROVE. Fail-closed: APPROVE only when every finding is
    KNOWN to be low/nit."""
    posts = []
    findings = [
        # Unknown severity — typo / new severity / model hallucination
        {"id": "X1", "severity": "blocker", "body": "concern",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
    ]
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings, posts)
    assert rc == 0  # not critical/high → not blocking-exit
    assert posts[0]["event"] == "COMMENT", (
        f"Unknown severity {findings[0]['severity']!r} must NOT yield APPROVE "
        f"(fail-closed); got {posts[0]['event']}"
    )
    # And a warning about the unrecognized severity was emitted.
    err = capsys.readouterr().err
    assert "unrecognized severity" in err
    assert "blocker" in err
    assert "withholding APPROVE" in err


def test_main_does_not_approve_on_missing_severity(monkeypatch, tmp_path):
    """A finding missing the severity field entirely → COMMENT (not
    APPROVE). Malformed model output should never grant the bot's
    approval."""
    posts = []
    findings = [
        {"id": "Y1", "body": "missing severity field",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
    ]
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings, posts)
    assert rc == 0
    assert posts[0]["event"] == "COMMENT"


def test_main_approves_with_uppercase_low_severity(monkeypatch, tmp_path):
    """Severity case-insensitive: 'LOW' / 'Low' / 'low' all count as
    advisory. Defensive against model emitting different casings."""
    posts = []
    findings = [
        {"id": "L1", "severity": "LOW", "body": "minor",
         "citation": "src/foo.py:2", "file": "src/foo.py", "line": 2},
    ]
    rc = _run_main_with_findings(monkeypatch, tmp_path, findings, posts)
    assert rc == 0
    assert posts[0]["event"] == "APPROVE"
