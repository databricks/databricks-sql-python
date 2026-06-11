#!/usr/bin/env python3
"""PR Review Bot orchestrator.

Entry point invoked by the workflow. Reads context from env vars,
runs all five phases, exits 0 on graceful skip / posted review, and
exits 1 on hard failure (after attempting to post a failure summary).

Env vars (all required unless noted):
  GITHUB_REPOSITORY            owner/repo (e.g., "databricks/databricks-driver-test")
  PR_NUMBER                    int
  HEAD_SHA                     full 40-char SHA
  DATABRICKS_TOKEN             LLM endpoint auth
  MODEL_ENDPOINT               full URL of the Databricks model serving endpoint
  GH_TOKEN                     gh CLI auth (built-in GITHUB_TOKEN)
  DRY_RUN                      "true" to skip the post step (default: "false")
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

from . import gather_context as gc
from . import post_review as pr
from . import reconcile as rc
from . import validate_findings as vf
from .post_review import BOT_LOGINS as REVIEW_BOT_LOGINS
from scripts.shared import llm_client, sdk_agent
from .agent import run_agent, AgentLoopError
from .markers import inline_marker_for
from .observer import Observer
from .prompts import SYSTEM_PROMPT, OUTPUT_SCHEMA_DOC, USER_PROMPT_TEMPLATE
from .severity import BADGE, Severity, INLINE_ELIGIBLE


REPO_ROOT = Path(__file__).resolve().parents[2]


def _content_root() -> Path:
    """Filesystem root the bot READS PR-head content from — repo rules,
    touched specs/tests, and the agent's explore tools (read_paths/grep).

    Deliberately distinct from the directory the bot CODE runs in. On the
    `workflow_dispatch` trigger the workflow checks the PR head into a SEPARATE
    directory and exports its path as REVIEW_CONTENT_ROOT, while
    `python -m scripts.reviewer_bot.run_review` still executes from the trusted
    default-branch checkout. That split is a security boundary: workflow_dispatch
    is exempt from the fork guard, so if the bot ran the PR's own copy of
    scripts/reviewer_bot/* it would execute fork-authored code with org secrets
    (DATABRICKS_TOKEN, the minted App token) in scope. Reading the PR's file
    *content* is safe (it's the point of a review, and read_paths/grep enforce
    path-escape/.git/symlink guards); executing its code is not.

    When REVIEW_CONTENT_ROOT is unset (the `pull_request` trigger), fall back to
    REPO_ROOT — there the primary checkout already IS the PR (merge-ref) tree and
    the fork guard gates untrusted code, so "read root == the checkout the module
    runs from" is correct.
    """
    override = os.environ.get("REVIEW_CONTENT_ROOT")
    if override:
        return Path(override).resolve()
    return REPO_ROOT


import re as _re


def truncate_diff_to_hunk_boundary(diff_text: str, byte_cap: int) -> str:
    """Cap diff to ~byte_cap bytes, but only at hunk boundaries.

    Mid-hunk truncation breaks gather_context.parse_diff_positions
    (right-line counter advances off the dropped lines), so we never
    cut inside a hunk. We accumulate hunks until the next one would
    push us over the cap, then append a trailing comment noting how
    many hunks were dropped.

    Implementation: split on hunk-header boundaries (`\\n@@ `). The
    resulting list looks like:
        [parts[0]] = file headers BEFORE the first hunk header
                     (no hunk body — the @@ header starts at parts[1])
        [parts[1]] = first hunk's body (the `-x,y +a,b @@` line minus the
                     `@@ ` prefix that the separator consumed, then the
                     hunk body)
        [parts[2]] = second hunk's body
        ...
    We always include parts[0] AND parts[1] (first hunk) — accepting
    overrun if needed — because parse_diff_positions needs at least one
    hunk to map right-side lines. Returning just parts[0] (file headers
    only) would route every diff-anchored finding to the summary.

    We re-join the kept parts with the same `\\n@@ ` separator so the
    output is a valid unified diff.
    """
    if len(diff_text) <= byte_cap:
        return diff_text

    SEP = "\n@@ "
    parts = diff_text.split(SEP)
    if len(parts) <= 1:
        # No hunk boundary found — fall back to a raw cap so we still
        # honour the cap. This branch shouldn't fire for real diffs.
        return diff_text[:byte_cap]

    # Always keep parts[0] (file headers) + parts[1] (first hunk) — even
    # if that overruns the cap. parse_diff_positions REQUIRES at least
    # one hunk to populate the right-line mapping; returning bare file
    # headers would silently route every diff-anchored finding to the
    # summary section.
    kept = [parts[0], parts[1]]
    used = len(parts[0]) + len(SEP) + len(parts[1])

    for seg in parts[2:]:
        added = len(SEP) + len(seg)
        if used + added > byte_cap:
            break
        kept.append(seg)
        used += added

    dropped = len(parts) - len(kept)
    out = SEP.join(kept)
    if dropped:
        out += (
            f"\n[truncated: {dropped} hunk(s) omitted to fit "
            f"{byte_cap:,}-byte cap]\n"
        )
    return out


def build_summary_body(
    findings: list[dict],
    *,
    llm_summary: str,
) -> str:
    """Compose the PR-review body.

    Under the v2 posting model this is the `body` field of the
    `POST /pulls/{n}/reviews` payload — NOT a sticky issue comment.
    Inline findings are bundled into the `comments[]` array of the
    same review (handled by the caller), so this body intentionally
    repeats the verdict + every summary-routed finding. That set is
    Nit (Nit is never inline-eligible by design) PLUS any
    Critical/High/Medium/Low whose file:line was NOT diff-anchorable
    in `parse_diff_positions` — those land in the "Other findings"
    section so they remain visible even though they can't be inlined.

    There is no `failed_inline_ids` parameter: the bundled
    `POST /pulls/{n}/reviews` is all-or-nothing, so there's no
    per-finding failure list to render. If the whole POST fails the
    reactive retry in `main()` re-posts the body alone, so the
    verdict still lands.
    """
    if not findings:
        return "✅ No issues identified by the review bot."

    counts = Counter(f.get("severity", "unknown") for f in findings)
    verdict_parts = []
    for sev in ("critical", "high", "medium", "low", "nit"):
        if counts.get(sev):
            verdict_parts.append(f"{counts[sev]} {sev.title()}")
    if verdict_parts:
        verdict = " · ".join(verdict_parts)
    else:
        # All findings have unknown / unrecognized severity. The literal
        # "No findings" verdict is a lie — count them instead so the
        # reviewer sees the bot did flag something even if the buckets
        # didn't recognize the severity strings.
        n = len(findings)
        verdict = f"{n} finding{'s' if n != 1 else ''}"

    sections = [f"**Verdict:** {verdict}", "", llm_summary or ""]

    summary_findings = [f for f in findings if f.get("_route") == "summary"]
    if summary_findings:
        sections.append("\n### Other findings\n")
        for f in summary_findings:
            try:
                badge = BADGE.get(Severity(f.get("severity")), "")
            except (ValueError, TypeError):
                badge = ""
            sections.append(f"- {badge} — {f.get('body', '')}")

    return "\n".join(sections)


def _run_gh(args: list[str]) -> str:
    result = subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True
    )
    return result.stdout


# Tail of stderr to include in failure messages — long enough to be useful,
# short enough to fit comfortably in the sticky summary's `reason` slot.
_FAILURE_STDERR_TAIL = 500


def _driver_clone_url() -> str:
    """Return the driver-repo clone URL.

    Single source of truth for the URL so tests and the call site stay in
    sync. Auth (when needed) is passed via an `http.<url>.extraheader`
    config flag at clone time — NOT embedded in this URL — because URL
    embedding leaks the token into process argv and into `.git/config`'s
    `origin` URL. See the call site in `main()` for the extraheader wiring.
    """
    return "https://github.com/adbc-drivers/databricks.git"


def _require_env(name: str) -> str:
    """Read a required env var, raising a clear error if it's missing.

    Used for lazy reads of `MODEL_ENDPOINT` / `DATABRICKS_TOKEN` so
    early-exit paths (stale-SHA skip, dry-run smoke tests that never
    reach the agent) don't KeyError just because production creds
    aren't in the environment. The error is caught by the Phase 2 /
    Phase 3 try/except wrappers and routed through `_post_failure`,
    so a missing env var on a real run still surfaces visibly on the
    PR instead of silently exiting.
    """
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"required env var {name!r} is not set; needed by the "
            f"agent/planner phases"
        )
    return val


def _truncate_pr_body(body: str, cap: int = 4000) -> str:
    """Cap pr_body length and surface a visible truncation marker so the
    model knows the description was clipped (otherwise it can't tell
    whether absent context is genuinely missing or just trimmed)."""
    if len(body) <= cap:
        return body
    dropped = len(body) - cap
    return body[:cap] + f"\n\n[... truncated, {dropped} more chars ...]"


def main() -> int:
    repo = os.environ["GITHUB_REPOSITORY"]
    pr_number = int(os.environ["PR_NUMBER"])
    head_sha = os.environ["HEAD_SHA"]
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    # MODEL_ENDPOINT / DATABRICKS_TOKEN are NOT read here: they're only
    # needed by _plan_driver_files (Phase 2.5) and run_agent (Phase 3),
    # both of which run after the stale-SHA early-exit and the
    # non-csharp early-skip. Reading them at the top of main() would
    # KeyError on dry-run / smoke-test paths that never hit those
    # phases. They're read lazily via _require_env() at each call site.

    observer = Observer(
        pr_number=pr_number,
        head_sha=head_sha,
        trigger_event=os.environ.get("GITHUB_EVENT_NAME", "unknown"),
    )
    observer.log(f"PR Review Bot starting on PR #{pr_number} @ {head_sha[:12]}")

    # ── Phase 2: gather context ─────────────────────────────────────
    # Wrap Phase 2 in a try/except so failures route through _post_failure
    # (and leave a sticky comment on the PR) instead of crashing main()
    # before the failure-handler is in scope. Any uncaught exception here
    # used to just bubble out — the PR would see no signal that the bot
    # ran at all.
    try:
        pr_meta = json.loads(_run_gh([
            "pr", "view", str(pr_number), "--repo", repo,
            "--json", "title,body,url,headRefOid,baseRefOid,files",
        ]))
        # Stale-SHA guard at the START of the run, too — saves the
        # LLM call if the PR has already moved past this commit.
        # Compare against the trigger-time head_sha env var (the SHA at
        # workflow-dispatch time); comparing against pr_meta['headRefOid']
        # would be tautological (it came from this same `gh pr view`).
        if pr_meta["headRefOid"] != head_sha:
            msg = (
                f"head_sha changed since trigger "
                f"({head_sha[:12]} → {pr_meta['headRefOid'][:12]})"
            )
            print(f"::notice::{msg}. Next synchronize event will re-run.")
            observer.record_skip(f"stale SHA at start: {msg}")
            observer.write_step_summary()
            return 0

        changed_files = [f["path"] for f in pr_meta["files"]]

        # ── One diff, two consumers ───────────────────────────────────
        #
        # Fetch the PR's diff and parse it once. The same diff text is
        # used for BOTH (a) `parse_diff_positions` (inline-comment
        # routing — matches what GitHub's PR-review API validates
        # against, so an inline anchor that passes our check also
        # passes GitHub's) AND (b) the LLM prompt content.
        #
        # We don't narrow to a since-last-review subset. The "P2"
        # narrowing (`<prior_review_sha>..<head>`) shipped briefly
        # and proved costly: when a branch merged main between
        # reviews, the narrowed diff included files that were part of
        # main's state but not THIS PR's contribution, and the LLM
        # reviewed those instead of the actual PR. Three bugs traced
        # to that mismatch (orphan pending review, inline routing 422,
        # merge-from-base scope) cost more to fix than the saved
        # input tokens were ever worth. P1 — dedup against open
        # threads (see below) — does the noise-reduction work
        # without depending on diff narrowing.
        diff = _run_gh(["pr", "diff", str(pr_number), "--repo", repo])
        # MUST truncate on hunk boundaries — a raw slice mid-hunk leaves
        # parse_diff_positions with a stale right-line counter and breaks
        # inline-routing for everything after the cut.
        diff = truncate_diff_to_hunk_boundary(diff, 200_000)
        diff_positions = gc.parse_diff_positions(diff)
        observer.log("diff scope: full PR diff (base..head)")

        # Read PR-head content from the content root (the separate pr-head/
        # checkout on workflow_dispatch; the module's own checkout otherwise).
        # NOT from REPO_ROOT directly — see _content_root() for the security
        # rationale (workflow_dispatch is fork-guard-exempt).
        content_root = _content_root()
        repo_rules = gc.aggregate_repo_rules(changed_files, content_root)

        # Driver clone (csharp only in v1).
        # Trigger on tests/csharp/ OR specs/*.yaml — the bot validates
        # spec/test pairs, so a spec change can require driver-source context
        # even if no test file was touched in this PR.
        driver_clone: Path | None = None
        driver_listing = "(driver not cloned for this PR)"
        driver_prefetched_files = "(driver not cloned for this PR)"
        needs_driver_clone = any(
            f.startswith("tests/csharp/")
            or (f.startswith("specs/") and f.endswith(".yaml"))
            for f in changed_files
        )
        if needs_driver_clone:
            runner_temp = Path(os.environ.get("RUNNER_TEMP", "/tmp"))
            driver_clone = runner_temp / "driver-clone"
            if not driver_clone.exists():
                # Use the GitHub App token (INTEGRATION_TEST_APP_TOKEN)
                # when set so the clone keeps working if the driver repo
                # ever goes private. Falls back to public HTTPS otherwise.
                #
                # The token is NEVER embedded in the URL — that would land
                # it in process argv and in `.git/config`'s `origin` URL.
                # Instead pass it via `http.<url>.extraheader` so it lives
                # only in git's HTTPS request headers for this single clone
                # operation, then defensively `remote set-url` afterwards
                # to ensure no auth metadata persists in `.git/config`.
                app_token = os.environ.get("INTEGRATION_TEST_APP_TOKEN")
                clone_url = _driver_clone_url()
                clone_cmd = ["git"]
                if app_token:
                    # `credential.helper=` (empty) disables any system /
                    # global credential helpers — prevents prompts and
                    # prevents helpers from caching the token.
                    clone_cmd += [
                        "-c", "credential.helper=",
                        "-c",
                        f"http.{clone_url}.extraheader="
                        f"AUTHORIZATION: bearer {app_token}",
                    ]
                clone_cmd += [
                    "clone", "--depth", "1", clone_url, str(driver_clone),
                ]
                try:
                    subprocess.run(
                        clone_cmd,
                        check=True, capture_output=True, text=True,
                    )
                except subprocess.CalledProcessError as e:
                    # Bubble up a clear, redacted reason — never include
                    # the token if it appears in stderr.
                    stderr_tail = (e.stderr or "").strip()[-_FAILURE_STDERR_TAIL:]
                    if app_token:
                        stderr_tail = stderr_tail.replace(app_token, "<TOKEN>")
                    raise RuntimeError(
                        f"Driver clone failed: {stderr_tail}"
                    ) from e
                # Defensive: even though we used extraheader (not a
                # tokenized URL), explicitly reset the remote URL so no
                # stale auth ever lands in `.git/config`. Safe to run
                # unconditionally — sets the remote to the canonical
                # public HTTPS URL.
                try:
                    subprocess.run(
                        ["git", "-C", str(driver_clone),
                         "remote", "set-url", "origin", clone_url],
                        check=False, capture_output=True, text=True,
                    )
                except Exception:  # noqa: BLE001 — best-effort hygiene
                    pass
            driver_listing = gc.list_driver_source(
                driver_root=driver_clone,
                source_subpath="csharp/src/",
            )

        # Cheap heuristic gate: skip the planner LLM call when the PR
        # clearly won't need driver source. The planner is *meant* to
        # reduce total LLM round-trips by pre-fetching files the agent
        # would otherwise fetch via `read_paths`, but for spec-only
        # PRs (no `.cs` files changed) the agent typically needs zero
        # driver-source reads anyway — forcing a planner round-trip
        # there adds latency and rate-limit pressure with no payoff.
        # (Combined with llm_client's 60 s 429-retry sleep, an
        # unconditional planner call could block the whole review for
        # up to 60 s × (retries + 1) before the main agent even
        # starts.)
        #
        # Heuristic: any `.cs` file in `changed_files` triggers the
        # planner; otherwise skip and let the agent fall back to
        # `read_paths` on the rare spec-only PR that genuinely needs
        # driver context. This is intentionally conservative — a
        # tighter diff-size / .cs-reference scan would catch more
        # cases but is harder to reason about. Easy to relax later
        # if data shows spec-only PRs frequently need pre-fetched
        # files.
        planner_should_run = driver_clone is not None and any(
            f.endswith(".cs") for f in changed_files
        )
        if driver_clone is not None and not planner_should_run:
            observer.log(
                "Phase 2.5 (plan): skipped — no .cs files changed; "
                "agent will use read_paths if it needs driver source"
            )
            # The driver IS cloned for this PR — the planner was just
            # skipped by the .cs-files heuristic. Overwrite the initial
            # "(driver not cloned for this PR)" sentinel so the agent
            # prompt's `## Driver source files (pre-fetched ...)` block
            # gives accurate context. Without this, the prompt reads
            # "driver not cloned" right next to a directory listing
            # showing the tree exists — confusing the model.
            driver_prefetched_files = (
                "(planner skipped — no .cs files changed; "
                "use read_paths if you need driver source)"
            )
        if planner_should_run:
            # Lazy env-var read: only the agent / planner phases need
            # these. Reading them at the top of main() would KeyError
            # on dry-run / smoke-test paths that early-exit before
            # ever reaching Phase 2.5.
            planned_paths = _plan_driver_files(
                endpoint=_require_env("MODEL_ENDPOINT"),
                token=_require_env("DATABRICKS_TOKEN"),
                changed_files=changed_files,
                driver_listing=driver_listing,
            )
            driver_prefetched_files = _read_driver_files(driver_clone, planned_paths)
            # Report true UTF-8 byte size, not `len()` of the string \u2014
            # `len()` on a str is a character count, which under-reports
            # any non-ASCII content and was misleading the step-summary
            # metric (the suffix is `B`, i.e. bytes).
            prefetched_bytes = len(driver_prefetched_files.encode("utf-8"))
            observer.log(
                f"Phase 2.5 (plan): planned={len(planned_paths)} files  "
                f"prefetched={prefetched_bytes:,}B"
            )

        touched_text = _read_touched_specs_and_tests(changed_files, content_root)

        observer.record_phase_2(
            diff_bytes=len(diff),
            repo_rules_bytes=len(repo_rules),
            touched_specs_tests_bytes=len(touched_text),
            driver_cloned=driver_clone is not None,
            driver_file_count=(
                sum(1 for _ in (driver_clone / "csharp/src/").rglob("*") if _.is_file())
                if driver_clone is not None and (driver_clone / "csharp/src/").is_dir()
                else 0
            ),
            driver_listing_bytes=len(driver_listing) if driver_listing else 0,
        )
    except subprocess.CalledProcessError as e:
        # _run_gh failures (e.g., the initial `gh pr view` or `gh pr diff`).
        stderr_tail = (e.stderr or "").strip()[-_FAILURE_STDERR_TAIL:]
        reason = f"Phase 2 gh api failed: {stderr_tail or e}"
        observer.record_failure(reason)
        observer.write_step_summary()
        return _post_failure(repo, pr_number, reason, dry_run)
    except Exception as e:
        # Catch-all so the PR sees something useful instead of silence.
        # Includes the RuntimeError raised by the driver-clone failure
        # branch above and any other unexpected failure during Phase 2.
        reason = f"Phase 2 failed: {e}"
        observer.record_failure(reason)
        observer.write_step_summary()
        return _post_failure(repo, pr_number, reason, dry_run)

    # ── Phase 2.9: fetch open review threads for dedup context ────
    # Hand the LLM a one-line-per-thread summary of every UNRESOLVED
    # peco-review-bot thread on this PR. The system prompt tells the
    # model to skip emitting findings that match an open thread and
    # record them in a `suppressions[]` array for audit. On a
    # 100-commit PR with ~150 open threads (the PR #382 case), this is
    # the dedup layer that catches what the diff-since-last-review
    # scoping (P2 above) didn't already exclude — typically because a
    # single commit happened to re-touch a region with a prior open
    # finding.
    #
    # Failure mode: if the GraphQL call fails (auth, network, etc.),
    # fall back to "(no open review-bot threads)" so the LLM proceeds
    # without dedup rather than aborting the whole review. The dedup
    # gate is an optimization; missing it produces noise, not wrong
    # findings.
    try:
        open_threads_data = rc.fetch_open_review_threads(
            repo=repo, pr_number=pr_number,
        )
        # Pass the full BOT_LOGINS tuple (current + legacy) so this
        # also recognizes pre-app-migration PRs whose existing open
        # threads were authored as `github-actions[bot]`. Filtering on
        # only the current login would silently exclude those legacy
        # threads from the dedup prompt → the LLM re-emits duplicates
        # for findings that are already open. The marker check inside
        # post_review remains the security gate; this login set is
        # only for thread-author recognition.
        open_threads_text = gc.format_open_threads(
            open_threads_data, bot_login=REVIEW_BOT_LOGINS,
        )
        observer.log(
            f"dedup context: {len(open_threads_data)} open thread(s) on PR; "
            f"formatted for LLM prompt"
        )
    except Exception as e:
        observer.log(
            f"::warning::dedup context fetch failed ({e!r}); "
            f"proceeding without open-threads context"
        )
        open_threads_text = "(no open review-bot threads)"

    # ── Phase 3: agentic LLM loop ──────────────────────────────────
    user_prompt = USER_PROMPT_TEMPLATE.format(
        pr_title=pr_meta["title"],
        pr_url=pr_meta["url"],
        pr_body=_truncate_pr_body(pr_meta.get("body") or ""),
        diff=diff,
        open_threads=open_threads_text,
        repo_rules=repo_rules,
        touched_specs_and_tests=touched_text,
        driver_listing=driver_listing,
        driver_prefetched_files=driver_prefetched_files,
    ) + "\n\n" + OUTPUT_SCHEMA_DOC

    try:
        result = run_agent(
            endpoint=_require_env("MODEL_ENDPOINT"),
            token=_require_env("DATABRICKS_TOKEN"),
            system=SYSTEM_PROMPT, user=user_prompt,
            driver_root=driver_clone,
            content_root=content_root,
            observer=observer,
        )
    except Exception as e:
        # Includes AgentLoopError; covers any LLM call / tool execution
        # failure. _post_failure leaves a sticky summary so the PR shows
        # something useful instead of failing silently.
        observer.record_failure(str(e))
        observer.write_step_summary()
        return _post_failure(repo, pr_number, str(e), dry_run)

    raw_findings = result.get("findings") or []
    # The model can return malformed entries (e.g., a bare string mixed in
    # with dicts). Every downstream stage assumes `f` is a dict with
    # `.get(...)`. Filter at the boundary so a single bad entry doesn't
    # AttributeError-crash the whole run.
    findings = [f for f in raw_findings if isinstance(f, dict)]

    # Audit trail for the dedup gate (P1). The model emits a `suppressions`
    # array whenever it skipped a candidate finding because an open thread
    # already covers it. Log to workflow output + step summary so a human
    # can spot-check the bot's calls — false-positive dedups (legit new
    # finding suppressed) are hard to recover from silently, and the audit
    # is the cheap way to catch them.
    raw_suppressions = result.get("suppressions") or []
    suppressions = [s for s in raw_suppressions if isinstance(s, dict)]
    if suppressions:
        observer.log(
            f"dedup gate: {len(suppressions)} candidate finding(s) suppressed "
            f"against open threads"
        )
        for s in suppressions:
            observer.log(
                f"  [dedup] thread={s.get('thread_id')!r} "
                f"reason={s.get('reason')!r}"
            )
        _append_step_summary(
            "## Dedup suppressions (audit)\n\n"
            "The reviewer skipped these candidate findings because an "
            "open thread already covers them. If any look like real "
            "*new* issues that got wrongly merged, reply to the cited "
            "thread to re-trigger.\n\n"
            + "\n".join(
                f"- thread #{s.get('thread_id')}: {s.get('reason')}"
                for s in suppressions
            )
            + "\n"
        )
    if len(findings) < len(raw_findings):
        observer.log(
            f"::warning::Dropped {len(raw_findings) - len(findings)} non-dict "
            f"finding(s) from model output."
        )
    # Defensive truncate — SYSTEM_PROMPT already constrains the summary
    # to 2-3 sentences, but a model that ignores the prompt can't blow
    # up the review body if we cap here.
    llm_summary = vf.truncate_summary(result.get("summary") or "")

    # ── Phase 4: validate ──────────────────────────────────────────
    pre_demote = {f.get("id"): f.get("severity") for f in findings}
    findings = vf.demote_uncited_or_unverified(findings)
    demotions = [
        (fid, pre_demote.get(fid), f.get("severity"))
        for f in findings
        for fid in [f.get("id")]
        if pre_demote.get(fid) and pre_demote[fid] != f.get("severity")
    ]
    if demotions:
        _append_step_summary(
            "## Demotions applied by demote_uncited_or_unverified\n\n"
            + "\n".join(
                f"- `{fid}`: {old} → {new}" for fid, old, new in demotions
            )
            + "\n\n(Demoted findings either lacked a citation or asserted "
            "absence without a verified_against field. They still post in the "
            "summary at the demoted severity, but are not eligible for inline.)\n"
        )
    findings = vf.dedupe_findings(findings)
    findings = vf.route_by_diff_position(findings, diff_positions)

    # Re-route Low/Nit findings whose `_route == "inline"` back to summary.
    # `route_by_diff_position` sets `_route` purely on file:line membership
    # in the diff; the posting loop separately filters to INLINE_ELIGIBLE
    # severities. Without this re-route, a Low/Nit with a diff-anchored
    # file:line ends up routed `inline` but is ALSO skipped at post time
    # because its severity isn't eligible — disappearing from both the
    # inline list AND the summary section. Re-routing here makes them
    # visible in `Other findings`.
    eligible_sev_names = frozenset(s.value for s in INLINE_ELIGIBLE)
    for f in findings:
        if (
            f.get("_route") == "inline"
            and f.get("severity") not in eligible_sev_names
        ):
            f["_route"] = "summary"

    # Anchor-rule violations: surface findings the LLM emitted without
    # file:line despite the system-prompt's hard rule that every
    # non-Nit finding must anchor. These got routed `summary` by
    # `route_by_diff_position` (because no file+line to match against
    # diff_positions), but operators won't see WHY — the rendered
    # review just shows them in the "Other findings" block alongside
    # legitimately summary-routed meta-findings. Logging a warning per
    # violation gives us the signal to either tighten the prompt
    # further or investigate model-output drift, without breaking the
    # post.
    for f in findings:
        sev = f.get("severity")
        if (
            sev in eligible_sev_names
            and f.get("_route") == "summary"
            and (not f.get("file") or not isinstance(f.get("line"), int))
        ):
            print(
                f"::warning::anchor-rule violation: finding "
                f"{f.get('id')!r} ({sev}) routed to summary because "
                f"the finding has no usable file:line anchor. The "
                f"system prompt requires non-Nit findings to anchor. "
                f"Body preview: {(f.get('body') or '')[:120]!r}",
                file=sys.stderr,
            )

    # Pass the eligible-severity set so the observer's inline_count
    # matches what Phase 5 will actually post (Lows/Nits with diff
    # positions still go to summary).
    observer.record_validation(
        findings=findings,
        inline_eligible_severities=eligible_sev_names,
    )

    # ── Phase 5: post (or dry-run print) ───────────────────────────
    # Stale-SHA guard (second check, immediately before posting).
    # By this point we have a paid-for LLM result — a transient
    # `gh pr view` failure should NOT throw it away. Wrap the call so
    # we proceed with posting when the check itself errors out; the
    # SHA hasn't been confirmed to have changed, so the safer default
    # is to post rather than to silently discard the run.
    try:
        current_sha = _run_gh([
            "pr", "view", str(pr_number), "--repo", repo,
            "--json", "headRefOid", "--jq", ".headRefOid",
        ]).strip()
    except Exception as e:
        # Catch broadly — CalledProcessError (gh ran but returned non-zero),
        # FileNotFoundError (gh not on PATH), OSError (network/io), etc.
        # An uncaught exception here would bubble out of main(), bypassing
        # _post_failure entirely → PR sees no signal that the bot ran.
        # The next gh call (posting inline/sticky) will surface real issues
        # via its own error path; we just need to NOT discard the LLM result
        # here on a transient check failure.
        stderr_tail = getattr(e, "stderr", "") or ""
        stderr_tail = stderr_tail.strip()[-_FAILURE_STDERR_TAIL:]
        print(
            f"::warning::stale-SHA guard before post failed "
            f"({type(e).__name__}: {stderr_tail or e}); proceeding with posting.",
            file=sys.stderr,
        )
        current_sha = head_sha  # assume current; better than discarding
    if current_sha != head_sha:
        msg = f"head_sha changed during run ({head_sha[:12]} → {current_sha[:12]})"
        print(f"::notice::{msg}; skipping post.")
        observer.record_skip(f"stale SHA before post: {msg}")
        observer.write_step_summary()
        return 0

    # Build the inline-findings payload from validated findings. Each
    # entry mirrors what the v1 model used to POST individually, but
    # bundled into the single review-event payload.
    #
    # Bad model output can drop required fields — guard each access with
    # .get() / try-except so one malformed finding doesn't KeyError out
    # of payload construction.
    inline_payload: list[dict] = []
    for f in findings:
        if f.get("_route") != "inline":
            continue
        sev_str = f.get("severity")
        try:
            sev = Severity(sev_str)
        except (ValueError, TypeError):
            print(
                f"::warning::skipping finding with invalid severity "
                f"{sev_str!r} (id={f.get('id')!r})",
                file=sys.stderr,
            )
            continue
        if sev not in INLINE_ELIGIBLE:
            continue
        fid = f.get("id")
        path = f.get("file")
        line = f.get("line")
        body_text = f.get("body")
        if not (fid and path and isinstance(line, int) and body_text):
            print(
                f"::warning::skipping inline finding missing required fields "
                f"(id={fid!r} file={path!r} line={line!r})",
                file=sys.stderr,
            )
            continue
        # F2 proactive validation: the bundled POST /pulls/{n}/reviews
        # is all-or-nothing; a single inline entry whose (path, line) is
        # not in diff_positions 422s the WHOLE review (body + every
        # other inline). route_by_diff_position already filtered, but
        # the model could still hallucinate file/line, so re-verify
        # against diff_positions here and re-route any non-anchorable
        # finding to summary instead of dropping it.
        if line not in diff_positions.get(path, set()):
            print(
                f"::warning::inline finding {fid!r} at {path}:{line} not "
                f"in diff_positions; re-routing to summary to avoid 422 "
                f"on the bundled review POST.",
                file=sys.stderr,
            )
            f["_route"] = "summary"
            continue
        badge = BADGE[sev]
        marker = inline_marker_for(fid)
        inline_payload.append({
            "path": path,
            # GitHub's pulls/{n}/reviews API: use `line` (file line number)
            # + `side: "RIGHT"` (added/modified side of the diff). The
            # legacy `position` field (diff offset) is deprecated for this
            # endpoint and rejects with 422 when we passed a file line.
            "line": line,
            "side": "RIGHT",
            "body": f"{badge} — {body_text}\n\n{marker}",
        })

    review_body = build_summary_body(findings, llm_summary=llm_summary)

    if dry_run:
        print("=== DRY RUN: findings ===")
        print(json.dumps(findings, indent=2))
        print("=== DRY RUN: review body ===")
        print(review_body)
        print("=== DRY RUN: inline comments ===")
        print(json.dumps(inline_payload, indent=2))
        # Dry-run posts NOTHING. record_posting with posted_inline=len(...)
        # was misleading — the step summary then claimed inline comments
        # had been posted when none had. Report 0/0 and surface the
        # would-be-inline count on a log line for diagnostic visibility.
        observer.log(
            f"Phase 5 (dry-run): would_post_inline={len(inline_payload)} "
            f"(no GitHub calls made)"
        )
        observer.record_posting(
            deleted_prior_inline=0,
            posted_inline=0,
            failed_inline=0,
            review_action="skipped (dry_run)",
        )
        observer.write_step_summary()
        return 0

    # ── v2 posting: single PR-review event ─────────────────────────
    # One `POST /pulls/{n}/reviews` call bundling the summary body +
    # every inline finding. No sticky-comment patching, no per-finding
    # POSTs, no pre-cleanup of prior bot comments — each review is a
    # discrete timeline event. Old-thread cleanup is handled out-of-
    # band by reviewer_bot/followup.py.
    #
    # Decide the review event:
    #   APPROVE  — only when every finding is in the advisory tier
    #              (low, nit). Empty findings list also approves.
    #              An APPROVE counts toward branch protection's
    #              "Require N approvals" gate, enabling the bot to be a
    #              required reviewer (when wrapped in a single-member team).
    #   COMMENT  — any Medium/High/Critical present, OR any finding with
    #              an unrecognized severity (typo, missing severity field,
    #              new severity added later). Fail-closed: better to
    #              withhold approval than silently approve on a severity
    #              the runner doesn't recognize.
    #
    # REQUEST_CHANGES is not used; the workflow-level exit-non-zero gate
    # (later in this function, for Critical/High) is the merge-blocking
    # mechanism — `event: REQUEST_CHANGES` would gate even when humans
    # are willing to override, which is less flexible.
    _ADVISORY_SEVERITIES = {"low", "nit"}
    all_advisory = all(
        isinstance(f, dict)
        and (f.get("severity") or "").lower() in _ADVISORY_SEVERITIES
        for f in findings
    )
    # Log a warning for any unrecognized severity so we notice when the
    # runner falls through to COMMENT because of malformed model output.
    for f in findings:
        if not isinstance(f, dict):
            continue
        sev = (f.get("severity") or "").lower()
        if sev and sev not in _ADVISORY_SEVERITIES and sev not in (
            "critical", "high", "medium",
        ):
            print(
                f"::warning::finding id={f.get('id')!r} has unrecognized "
                f"severity {f.get('severity')!r}; treating as concern "
                f"(withholding APPROVE).",
                file=sys.stderr,
            )
    review_event = "APPROVE" if all_advisory else "COMMENT"

    # Defense against the "one pending review per PR" trap. Pending
    # reviews can be left behind by:
    #   1. A failed POST /pulls/{n}/reviews where GitHub created the
    #      underlying draft before the submit step rejected (rare in
    #      v2 since we always send `event` — but observed in the
    #      original P2 bug where `gh api -F` accidentally turned a
    #      GET into a POST against this endpoint).
    #   2. A future caller that legitimately wants a pending review
    #      but never submits it.
    # Either way, leftover pending reviews block ALL subsequent
    # `post_pr_review` calls on the same PR with a 422 — and only the
    # bot's own auth can see/delete them (pending reviews are private
    # to their author). Clean up before posting so the bot can always
    # make forward progress.
    cleared = pr.cleanup_orphan_pending_reviews(
        repo=repo, pr_number=pr_number,
    )
    if cleared:
        observer.log(
            f"cleanup_orphan_pending_reviews: removed {cleared} orphan "
            f"pending review(s) before post"
        )

    review_ok = pr.post_pr_review(
        repo=repo,
        pr_number=pr_number,
        head_sha=head_sha,
        body=review_body,
        inline_findings=inline_payload,
        event=review_event,
    )

    # F2 reactive retry: if the first POST failed AND we sent inline
    # comments, the failure is almost certainly a per-comment 422 (an
    # entry whose path/line slipped past the proactive validation, e.g.
    # the line is in diff_positions but GitHub rejects it for some other
    # reason). The body-only post almost always lands — retry once
    # without the comments[] array so at least the verdict + summary
    # findings reach the PR before we give up.
    #
    # Before retrying: re-route every inline finding to summary and
    # REBUILD the review body, so the dropped findings still appear in
    # the body's "Other findings" section. Without this, findings would
    # be counted in the verdict but their bodies would vanish from the
    # PR entirely — exactly the bug Copilot flagged on PR #327.
    retried_body_only = False
    if not review_ok and inline_payload:
        print(
            "::warning::review post failed with inline comments; "
            "re-routing inline findings to summary and falling back "
            "to body-only post",
            file=sys.stderr,
        )
        # Mutate `_route` on the original findings so build_summary_body
        # picks them up under "Other findings". The findings list is
        # the same dict objects that inline_payload was derived from.
        rerouted_ids = []
        for f in findings:
            if isinstance(f, dict) and f.get("_route") == "inline":
                f["_route"] = "summary"
                rerouted_ids.append(f.get("id"))
        if rerouted_ids:
            print(
                f"::warning::re-routed {len(rerouted_ids)} inline finding(s) "
                f"to summary for body-only retry: {rerouted_ids}",
                file=sys.stderr,
            )
        review_body = build_summary_body(findings, llm_summary=llm_summary)
        # Preserve the original event on retry — a clean review stays an
        # APPROVE even if the inline POST failed (the findings are still
        # in the body via "Other findings").
        review_ok = pr.post_pr_review(
            repo=repo,
            pr_number=pr_number,
            head_sha=head_sha,
            body=review_body,
            inline_findings=[],
            event=review_event,
        )
        retried_body_only = review_ok

    if not review_ok:
        observer.log(
            "::error::PR review post failed — see prior "
            "::warning::post_pr_review log line."
        )
        observer.record_failure("PR review post failed")

    # `posted_inline` counts what GitHub accepted; the body-only retry
    # drops them, so report 0 there. `failed_inline` counts the inline
    # entries that didn't reach the PR (the original inline_payload size
    # when we retried or hard-failed, 0 on first-try success).
    inline_posted = len(inline_payload) if (review_ok and not retried_body_only) else 0
    inline_failed = len(inline_payload) - inline_posted
    if retried_body_only:
        review_action = "post (body-only retry)"
    elif review_ok:
        review_action = "post"
    else:
        review_action = "post (failed)"
    observer.record_posting(
        deleted_prior_inline=0,  # v2 model never deletes prior comments
        posted_inline=inline_posted,
        failed_inline=inline_failed,
        review_action=review_action,
    )

    # Note: there used to be a "Phase 6: reconcile stale bot threads"
    # step here that walked the bot's OPEN finding threads and resolved
    # any whose finding wasn't re-raised in this run. It was removed
    # because the followup workflow already closes the loop with
    # higher accuracy: when engineer-bot pushes a fix and posts a
    # "Pushed <sha>" reply, the pull_request_review_comment event
    # fires reviewer_bot/followup.py, which SHA-verifies the claimed
    # fix against the finding and emits <retract>/<agree_and_suggest>
    # (both of which call auto_resolve). Followup also handles human
    # replies. The gap reconcile filled was "no reply ever arrived
    # because engineer-bot couldn't run" (workflow disabled, model
    # outage, action_required gate); on those degraded paths, stale
    # threads now stay open until a human resolves them — acceptable
    # given the operator already needs to intervene to fix the root
    # cause.

    # Gate on blocking findings: if the bot found any Critical or High,
    # exit non-zero so branch protection can require this check before
    # merge. Medium/Low/Nit DO NOT block — bot is advisory at those levels.
    #
    # The review still got posted regardless of the exit code; this
    # only affects the workflow's success/failure status. Resolving a
    # blocking finding requires the bot to retract on a subsequent run
    # (via the followup workflow) or a human override (admin bypass /
    # branch-protection exemption).
    # Case-insensitive: match the APPROVE gate's case-insensitive
    # comparison. Without this, a model emitting "High" or "HIGH" would
    # bypass this exit-non-zero gate (workflow exits 0) while ALSO
    # failing the APPROVE gate (event=COMMENT). The merge would still
    # pass the required-check gate even though the bot refused to
    # approve — silent gap in the bot-gating UX. Both gates must
    # recognize severity identically.
    blocking = sum(
        1 for f in findings
        if isinstance(f, dict)
        and (f.get("severity") or "").lower() in ("critical", "high")
    )
    if blocking > 0:
        # GitHub Actions workflow commands (`::error::`) are parsed from
        # STDOUT, not stderr. Writing the annotation to stderr leaves it
        # in the raw log but it won't render as a red annotation in the
        # job summary. The error line MUST be on stdout for the annotation
        # to appear next to the failing step.
        print(
            f"::error::Bot found {blocking} blocking finding(s) "
            f"(Critical/High). Branch protection should block merge "
            f"until these are addressed. See the posted review for details."
        )
        # Record in the observer BEFORE writing the step summary so the
        # summary's Outcome row shows "⚠️ FAILED" instead of the default
        # "Posted review" status — without this the operator sees a
        # green-looking step summary alongside a red workflow check.
        observer.record_failure(
            f"{blocking} blocking finding(s) (Critical/High)"
        )
        observer.write_step_summary()
        return 1

    observer.write_step_summary()
    return 0


def _compact_driver_listing(driver_listing: str, *, byte_cap: int = 20_000) -> str:
    """Strip per-file sizes and cap the driver listing for the planner.

    `gather_context.list_driver_source` emits lines like
    `csharp/src/Foo.cs  (1234 bytes)` — the planner only needs the path, so
    the trailing `  (N bytes)` suffix is dead weight. For large driver
    trees, the raw listing can be tens of KB and would itself drive
    rate-limit pressure on the planner call (which the planning round-trip
    is supposed to reduce, not add to). Truncate at a hard byte cap and
    leave a visible marker so the model knows entries were dropped.
    """
    if not driver_listing:
        return driver_listing
    # Strip the `  (N bytes)` suffix from every line. The regex anchors
    # at end-of-line so an unrelated `(... bytes)` substring inside a
    # path (extremely unlikely, but not impossible) won't be eaten.
    stripped = _re.sub(r"  \(\d+ bytes\)$", "", driver_listing, flags=_re.MULTILINE)
    if len(stripped.encode("utf-8")) <= byte_cap:
        return stripped
    # Cap at a line boundary so the planner doesn't see a half-path.
    lines = stripped.splitlines()
    kept: list[str] = []
    used = 0
    for line in lines:
        added = len(line.encode("utf-8")) + 1  # +1 for the join newline
        if used + added > byte_cap:
            break
        kept.append(line)
        used += added
    dropped = len(lines) - len(kept)
    out = "\n".join(kept)
    if dropped:
        out += (
            f"\n[truncated: {dropped} path(s) omitted to fit "
            f"{byte_cap:,}-byte planner cap]"
        )
    return out


def _plan_driver_files(
    *,
    endpoint: str,
    token: str,
    changed_files: list[str],
    driver_listing: str,
) -> list[str]:
    """Lightweight LLM call that returns which driver source files to pre-fetch.

    Sends only the PR's changed file paths and a compacted driver directory
    listing (path-only, capped) — no diff, no repo rules — so the input
    stays a few KB even for large driver trees. The raw listing from
    `list_driver_source` is unbounded and includes per-file sizes; passing
    it verbatim would defeat the rate-limit relief this planner is meant
    to provide. The model responds with a short JSON list of paths. On any
    failure returns an empty list so the review still proceeds (with tool
    calls as fallback).
    """
    system = (
        "You are helping plan a code review. "
        "Given the list of files changed in a pull request and the driver "
        "source directory listing, identify which driver source files the "
        "reviewer will need to read. "
        'Respond with JSON only: {"files": ["csharp/src/Foo.cs", ...]}. '
        'If no driver files are needed, respond with {"files": []}. '
        "Always return a JSON object with a `files` key — never a bare array. "
        "List specific file paths only — no directories."
    )
    compact_listing = _compact_driver_listing(driver_listing)
    user = (
        "Changed files in this PR:\n"
        + "\n".join(f"  {f}" for f in changed_files)
        + "\n\nDriver source listing (paths only):\n"
        + compact_listing
    )
    try:
        # Single-shot planning call (no tools) via the SDK; replaces the
        # hand-rolled call_llm POST. Best-effort — failure → [] → Phase 3 falls
        # back to the model exploring via read_paths/grep.
        result = sdk_agent.run_agent(
            endpoint=endpoint, token=token, model="databricks-claude-opus-4-8",
            system=system, prompt=user,
            cwd=os.getcwd(), allowed_tools=[], mcp_servers={},
            max_turns=1,
        )
        content = result.final_text or ""
        # The system prompt asks for {"files": [...]} but a model that
        # ignores the schema can still return a bare array. Parse with
        # json.loads directly (after trimming common ```json fences) so
        # we accept both top-level shapes — `llm_client.extract_json_block`
        # only returns dicts and would raise ParseError on a bare array,
        # silently disabling the entire pre-fetch path. Fall back to
        # extract_json_block (with its brace-balancing logic) when the
        # response is messier — prose around the JSON, multiple objects,
        # etc.
        stripped = content.strip()
        if stripped.startswith("```"):
            # Drop the opening fence (```json or ```) and the closing
            # ``` if present, so json.loads sees only the payload.
            first_nl = stripped.find("\n")
            if first_nl != -1:
                stripped = stripped[first_nl + 1 :]
            if stripped.endswith("```"):
                stripped = stripped[: -3]
            stripped = stripped.strip()
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            # Messy response — fall back to the dict-only brace-balancing
            # extractor. If THAT raises (e.g., bare array buried in prose),
            # the outer except catches and we return [].
            parsed = llm_client.extract_json_block(content)
        if isinstance(parsed, list):
            files = parsed
        elif isinstance(parsed, dict):
            files = parsed.get("files") or []
        else:
            files = []
        return [f for f in files if isinstance(f, str)]
    except Exception as exc:
        print(f"::warning::driver file planning failed ({exc}); skipping pre-fetch.", flush=True)
        return []


def _read_driver_files(
    driver_root: Path,
    paths: list[str],
    *,
    byte_cap: int = 150_000,
    per_file_cap: int | None = None,
) -> str:
    """Read specific driver source files into a single string.

    Used to load the files identified by _plan_driver_files before the main
    review loop, so the agent can skip read_paths tool calls for them.
    Applies the same path-escape guard as the agent's read_paths tool.

    Per-file size filter: any file whose on-disk size exceeds ``per_file_cap``
    bytes is SKIPPED rather than partially read. Defaults to ``byte_cap`` so
    a single large file can still be included as long as it fits within the
    total budget — the total cap is the real guard against prompt bloat.
    Note: the total budget is measured in UTF-8 bytes of the full emitted
    section (header ``=== path ===\\n`` + body), so a file whose on-disk size
    is exactly ``per_file_cap`` may still push the budget slightly over due
    to header and encoding overhead; the total ``byte_cap`` is the hard limit.
    """
    if per_file_cap is None:
        per_file_cap = byte_cap
    if not paths:
        return "(no driver files identified for pre-fetch)"
    driver_root_resolved = driver_root.resolve()
    sections: list[str] = []
    # Track skipped paths and the reason they were skipped so a future
    # debugger can tell whether the planner consistently picks paths
    # outside the listed tree (or hallucinates files that don't exist).
    # The planner is unconstrained beyond the system prompt, so silent
    # rejection here would otherwise surface as the opaque
    # "(none of the planned files were readable)" fallback below.
    skipped: list[tuple[str, str]] = []
    # Byte budget is measured in UTF-8 bytes of the FULL emitted section
    # (header + body + the "\n\n" separator that join() will insert
    # between sections). `len(text)` on a str is a character count, which
    # under-reports any non-ASCII content; the parameter is named
    # `byte_cap` and the truncation marker says "byte", so the unit must
    # match. Including header/separator bytes prevents the cap from
    # silently overrunning by ~30 bytes per file.
    SEP_BYTES = len("\n\n".encode("utf-8"))
    used = 0
    for i, p in enumerate(paths):
        try:
            target = (driver_root / p).resolve()
        except (OSError, ValueError) as exc:
            skipped.append((p, f"resolve failed: {type(exc).__name__}"))
            continue
        if not target.is_relative_to(driver_root_resolved):
            skipped.append((p, "outside driver_root (path-escape guard)"))
            continue
        if not target.is_file():
            skipped.append((p, "not a regular file"))
            continue
        # Per-file size filter. Use stat().st_size (on-disk byte size) so we
        # can reject oversized files without ever reading their contents.
        try:
            on_disk_size = target.stat().st_size
        except OSError as exc:
            skipped.append((p, f"stat failed: {type(exc).__name__}"))
            continue
        if on_disk_size > per_file_cap:
            skipped.append(
                (p, f"file too large: {on_disk_size:,}B > {per_file_cap:,}B per-file cap")
            )
            continue
        # Read defensively: a single unreadable file (perms, decode
        # surfaced as OSError, transient I/O) must NOT abort prefetch.
        # Phase 2.5 is best-effort \u2014 the agent can still read_paths in
        # the main loop \u2014 so we log the skip and continue. Without
        # this guard, one bad file kills the whole review run.
        try:
            text = target.read_text(errors="replace")
        except (OSError, UnicodeError) as exc:
            skipped.append((p, f"read failed: {type(exc).__name__}"))
            continue
        section = f"=== {p} ===\n{text}"
        section_bytes = len(section.encode("utf-8"))
        # Account for the separator that join() will insert before this
        # section (only when there's already at least one section).
        added = section_bytes + (SEP_BYTES if sections else 0)
        if used + added > byte_cap:
            sections.append(f"=== {p} ===\n[TRUNCATED: {byte_cap:,}-byte cap reached]")
            for remaining_p in paths[i + 1:]:
                skipped.append((remaining_p, f"total {byte_cap:,}-byte budget exhausted"))
            break
        sections.append(section)
        used += added
    if skipped:
        # Single ::warning:: line keeps the workflow log compact while
        # still giving every skipped path + reason for diagnosis.
        rendered = "; ".join(f"{p!r} ({reason})" for p, reason in skipped)
        print(
            f"::warning::_read_driver_files skipped {len(skipped)} "
            f"planner-returned path(s): {rendered}",
            flush=True,
        )
    return "\n\n".join(sections) if sections else "(none of the planned files were readable)"


def _read_touched_specs_and_tests(
    changed_files: list[str], content_root: Path = REPO_ROOT
) -> str:
    """Concatenate full content of any modified specs/*.yaml or test files.

    Reads relative to ``content_root`` — the PR-head checkout. On
    workflow_dispatch that's the separate ``pr-head/`` dir (NOT the trusted
    default-branch checkout the module runs from); see ``_content_root()``.
    """
    sections: list[str] = []
    for path in changed_files:
        full = content_root / path
        if not full.is_file():
            continue
        if path.startswith("specs/") and path.endswith(".yaml"):
            sections.append(f"=== {path} ===\n{full.read_text()[:32_000]}")
        elif path.startswith("tests/") and not path.endswith(".md"):
            sections.append(f"=== {path} ===\n{full.read_text()[:32_000]}")
    return "\n\n".join(sections)


def _post_failure(repo: str, pr_number: int, reason: str, dry_run: bool) -> int:
    body = (
        f"⚠️ Review bot failed — see workflow logs.\n\n"
        f"Reason: `{reason[:200]}`"
    )
    if dry_run:
        print("=== DRY RUN: failure review ===")
        print(body)
        return 1

    # v2 posting: emit a fresh PR-review event with the failure body
    # and no inline comments. We need head_sha to anchor the review;
    # under v1 the sticky-comment POST didn't need a SHA, but
    # `POST /pulls/{n}/reviews` requires `commit_id` — passing an empty
    # string here returns 422 and nothing visible lands on the PR.
    #
    # Fall back to fetching the current head via `gh pr view` if
    # HEAD_SHA is empty (some orchestration paths fail before the env
    # var is set). If that ALSO fails, log ::error:: and skip the post
    # entirely — the workflow exit code (1) still reflects the failure
    # even when the PR itself can't be marked.
    head_sha = os.environ.get("HEAD_SHA", "")
    if not head_sha:
        try:
            head_sha = _run_gh([
                "pr", "view", str(pr_number), "--repo", repo,
                "--json", "headRefOid", "--jq", ".headRefOid",
            ]).strip()
        except Exception as e:  # noqa: BLE001 — best-effort fallback
            print(
                f"::error::Cannot post failure summary: no HEAD_SHA "
                f"available and PR head lookup failed "
                f"({type(e).__name__}: {e}).",
                file=sys.stderr,
            )
            return 1
        if not head_sha:
            print(
                "::error::Cannot post failure summary: no HEAD_SHA "
                "available and PR head lookup returned empty.",
                file=sys.stderr,
            )
            return 1

    pr.post_pr_review(
        repo=repo,
        pr_number=pr_number,
        head_sha=head_sha,
        body=body,
        inline_findings=[],
    )
    return 1


def _append_step_summary(text: str) -> None:
    """Append text to the GitHub step summary if available."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except OSError:
        pass  # Best-effort; never fail the run for a logging hiccup.


if __name__ == "__main__":
    sys.exit(main())
