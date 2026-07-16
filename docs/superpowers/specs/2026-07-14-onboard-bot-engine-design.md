# Onboard databricks-sql-python to the pinned databricks-bot-engine

**Date:** 2026-07-14
**Status:** Approved (pending spec review)
**Repo:** `databricks/databricks-sql-python`

> **AUTHORITATIVE — current state (read this, not the chronology below).**
> - **Engine pin:** `3963f4be76be3b4ac0382ca0d7b6bf9d051929e6` (latest
>   `databricks-bot-engine` main), defined in **ONE** place —
>   `.github/actions/bot-prelude` `engine-ref` default. `.github/` has no other
>   SHA.
> - **Wiring:** each bot is its **own job** calling the local `bot-prelude` →
>   `install-bot-engine` composites (the engine is pip-installed, PAT-free via an
>   engine-scoped App token). We do **not** `uses:` the engine's reusable
>   workflows — that doesn't resolve cross-repo.
> - The `## Engine pin` / `## Design` sections and the `d780b2d…`, `d24ca21`,
>   `b6205fb`, `d9cb1e6` SHAs below are the **superseded draft history**, kept
>   for context. Where they conflict with this banner, the banner wins.

## Problem

The repo currently runs the PR **reviewer bot** from *vendored* code under
`scripts/reviewer_bot/` + `scripts/shared/`, invoked as
`python -m scripts.reviewer_bot.*`. The upstream `databricks/databricks-bot-engine`
is the documented source of truth: consumers `pip install` it pinned to an
immutable `engine-ref` and call its **reusable workflows**, rather than copying
its code. The vendored copy has already drifted from the engine and will not pick
up engine fixes without hand-resync.

This work converts the repo to the documented consumer model, matching the
existing `databricks/databricks-driver-test` consumer.

## Goal

- Reviewer bot (initial review + follow-up) runs from the pinned engine via its
  reusable workflows, installed in REF mode.
- All vendored bot code removed; engine is the single source of truth.
- Trigger policy unchanged: every non-fork PR (no label gate).
- Repo-specific review guidance preserved as engine *additive* prompt.

Non-goals: adopting the **engineer bot** (bug-fix / coverage). Only the reviewer
is in scope, matching what the repo runs today.

## Engine pin

- **Engine SHA:** `d780b2da60bb1ac68bb5cd1acb7cabf495b3ff2d` (this section
  reflects the original reusable-workflow draft; the SHA actually installed by
  the shipped local-composite workflows is tracked in the Reviewer update below).
- The SHA is used in BOTH the `uses:` ref AND the `engine-ref:` input of each
  caller, so the workflow definition and the installed engine match. Never
  `@main` (force-pushable; the job carries secrets).

## Design

### 1. Workflows — thin callers of the engine reusables

**`.github/workflows/reviewer-bot.yml`** (replaces the current hand-rolled job):
```yaml
name: Reviewer Bot
on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review]
  workflow_dispatch:
    inputs:
      pr_number: { description: 'PR number to review', required: true, type: string }
      dry_run:   { description: 'Print instead of posting', required: false, default: 'true', type: string }
permissions:
  contents: read
  pull-requests: write
  id-token: write
jobs:
  review:
    uses: databricks/databricks-bot-engine/.github/workflows/reviewer-bot.reusable.yml@d780b2da60bb1ac68bb5cd1acb7cabf495b3ff2d
    with:
      engine-ref: d780b2da60bb1ac68bb5cd1acb7cabf495b3ff2d
      pr-number: ${{ inputs.pr_number }}
      dry-run: ${{ inputs.dry_run }}
    secrets:
      review-bot-app-id: ${{ secrets.REVIEW_BOT_APP_ID }}
      review-bot-app-private-key: ${{ secrets.REVIEW_BOT_APP_PRIVATE_KEY }}
      databricks-token: ${{ secrets.DATABRICKS_TOKEN }}
      databricks-host: ${{ secrets.DATABRICKS_HOST }}
      engine-pat: ${{ secrets.BOT_ENGINE_PAT }}
```

**`.github/workflows/reviewer-bot-followup.yml`** (replaces the current job):
```yaml
name: Reviewer Bot — Follow-up
on:
  pull_request_review_comment: { types: [created] }
  pull_request: { types: [synchronize] }
permissions:
  contents: read
  pull-requests: write
  id-token: write
jobs:
  followup:
    # No enablement `if:` — keep today's "every non-fork PR" policy. The engine
    # reusable's job `if:` still enforces the security floor (fork==false, PR
    # open) and loop-prevention lives in the engine's followup.py.
    uses: databricks/databricks-bot-engine/.github/workflows/reviewer-bot-followup.reusable.yml@d780b2da60bb1ac68bb5cd1acb7cabf495b3ff2d
    with:
      engine-ref: d780b2da60bb1ac68bb5cd1acb7cabf495b3ff2d
    secrets:
      review-bot-app-id: ${{ secrets.REVIEW_BOT_APP_ID }}
      review-bot-app-private-key: ${{ secrets.REVIEW_BOT_APP_PRIVATE_KEY }}
      databricks-token: ${{ secrets.DATABRICKS_TOKEN }}
      databricks-host: ${{ secrets.DATABRICKS_HOST }}
      engine-pat: ${{ secrets.BOT_ENGINE_PAT }}
```

The engine reusables own: the fork/open-PR security gate (job `if:`), App-token
mint, Python setup, JFrog-OIDC engine install, `MODEL_ENDPOINT` construction
(`https://$DATABRICKS_HOST/serving-endpoints/databricks-claude-opus-4-8/invocations`),
`workflow_dispatch` input validation, and (for `reviewer-bot`) the PR-head
content-root dispatch security. So the ~250 lines of hand-rolled gate / checkout /
pre-filter logic in the current workflows are removed.

### 2. `.bot/prompts/review/system.md` — repo-specific additive prompt

Verified against the engine source at the pinned SHA
(`reviewer_bot/run_review.py::_review_system_prompt`): the reviewer's **base**
system prompt is **engine-owned** (`SYSTEM_PROMPT` — it owns the output contract,
severity scale, anchoring, and dedup rules). The consumer file is read from the
default path `.bot/prompts/review/system.md` and **appended** as *additive* repo
guidance via `append_repo_guidance`. It is **optional** at the default path
(missing ⇒ engine base alone — no abort). It is read from the TRUSTED checkout
(REPO_ROOT), never the PR-head tree (fork prompt-injection guard).

> Note: the engine README describes this file as "the reviewer's *entire* system
> prompt (absent ⇒ runtime abort)." The **code** at this SHA disagrees — it is
> additive and optional. We follow the code.

We port only the *repo-specific* parts of the current vendored `SYSTEM_PROMPT`
and drop the engine-owned parts (severity scheme, anchor rules, output structure,
dedup, `finalize_review` contract), which the base already owns. Kept:
- "senior code reviewer for databricks-sql-python (the Databricks SQL connector
  for Python)" framing + the review axes tuned for this repo.
- Landmarks: `CONTRIBUTING.md` (PEP 8, 100-char limit, DCO sign-off), `README.md`,
  connector package under `src/databricks/`, tests under `tests/unit` (fast,
  mocked) + `tests/e2e` (warehouse integration).

No `config.yaml`, engineer prompts, or `context-repos.yaml` — none are read by the
reviewer path.

### 3. Deletions (vendored removal)

Delete:
- `scripts/reviewer_bot/` (runtime + tests)
- `scripts/shared/` (runtime + tests)
- `scripts/__init__.py` (only present to package the vendored bot; `dependency_manager.py` runs as a path script)
- `scripts/requirements-sdk.txt`
- `.github/actions/setup-claude-sdk/`
- `.github/workflows/sdk-smoke.yml` (the reviewer-bot *foundation* smoke test; the only consumer of `setup-claude-sdk`)
- `.github/workflows/reviewer-bot-unit-tests.yml` (its tests import the deleted `scripts.reviewer_bot` / `scripts.shared`)

**Keep** (used by unrelated CI — verified via grep):
- `.github/actions/setup-jfrog/` — used by `kernel-e2e.yml` and `setup-poetry`.
- `scripts/dependency_manager.py` — used by `code-quality-checks.yml`.

### 4. Secrets (provisioned by the maintainer, documented in the PR)

| Secret | Status | Purpose |
|---|---|---|
| `REVIEW_BOT_APP_ID` | exists | mint bot App token |
| `REVIEW_BOT_APP_PRIVATE_KEY` | exists | " |
| `DATABRICKS_TOKEN` | exists | model auth |
| `DATABRICKS_HOST` | **NEW** — replaces `MODEL_ENDPOINT` | engine builds `MODEL_ENDPOINT` from it |
| `BOT_ENGINE_PAT` | **NEW** | read access to the private engine repo (REF-mode install) |

The old `MODEL_ENDPOINT` secret is no longer consumed after this change.

## Testing / verification

- Static: `actionlint` on the new workflow YAML if available, else a YAML parse
  check; confirm no dangling references to deleted paths remain (grep).
- Runtime: the reusable `uses:` against the **private** engine repo cannot fully
  resolve in CI until `BOT_ENGINE_PAT` + `DATABRICKS_HOST` are added as secrets.
  End-to-end verification (a real PR review) happens after secrets are
  provisioned. This limitation is stated plainly in the PR description — the PR
  is not claimed "verified working end-to-end."

## Rollout

1. New branch off `main`; commit workflows + `.bot/` + deletions.
2. Open PR to `databricks/databricks-sql-python` with the secrets checklist.
3. Maintainer adds `DATABRICKS_HOST` + `BOT_ENGINE_PAT`.
4. Merge; first non-fork PR exercises the reviewer end-to-end.
5. Future engine updates: bump the SHA in both `uses:` and `engine-ref:` in
   lockstep across both workflows.

## Risks

- **Private-engine install:** REF mode fails without `BOT_ENGINE_PAT`; the engine
  action fails fast with a clear error. Documented.
- **README vs. code drift** on the review prompt semantics: resolved by following
  the code (additive/optional), recorded above.
- **Reusable `uses: ./…` cross-repo resolution:** the engine's reusables use local
  `./.github/actions/...` refs that resolve against the engine repo at the called
  ref (verified in the engine's own dogfood). No consumer action needed.

---

## Update (2026-07-15): both bots, App-auth, engineer-bot added

Scope expanded per maintainer: this repo is the **first clean consumer** of
databricks-bot-engine, adopting **both** bots and used to surface onboarding
friction back to the engine.

### Reviewer — flipped to PAT-free App-auth
Pinned to engine SHA `d9cb1e63dc31eed550194a29d1cfd7de9c9c793b` at the time of
this update (the pin has since advanced to `3963f4b` — see the banner at the top;
this paragraph is left as-written history). The pin selects the engineer author
flow from the issue **Type** — Bug ⇒ bug-fix, Task/Feature ⇒ task (the engine
renamed the old `enhancement` flow to `task`); earlier drafts pinned `d24ca217`,
then `b6205fb`, post-#100. Both reviewer
workflows pass `engine-auth: app` and **drop `engine-pat`**: the reusable mints a
short-lived, engine-scoped GitHub App installation token from the review-bot App
creds it already receives. No `BOT_ENGINE_PAT` secret. The same SHA is pinned in
all four bot workflows (bump them in lockstep).

### Engineer-bot — added (bug-fix flow)
- `engineer-bot.yml` (author) + `engineer-bot-followup.yml` — own jobs (not
  reusable-workflow callers), because they build/run the product. Each mints a
  SECOND, engine-scoped App token from the engineer-bot App creds and passes it
  as `engine-pat` to `install-bot-engine` / `bot-run` (PAT-free, REF mode).
- `environment: azure-prod` on both jobs so they read `DATABRICKS_HOST` /
  `DATABRICKS_TOKEN` (env-scoped secrets). Own jobs *can* declare an environment
  — the reviewer reusable-callers cannot (see friction #3).
- Explicit `poetry install` before the engine runs, so the connector's own deps
  are present for `poetry run python -m pytest tests/unit` self-verify (REF mode
  installs only the engine — see friction #1).
- `.bot/config.yaml` (`flow: bug-fix`, `bash_allowlist` incl.
  `[poetry, run, python, -m, pytest]`) + `engineer/system.md`, `engineer/user.md`,
  `engineer-followup/system.md` (ported from the engine dogfood, rewritten for the
  connector: `src/databricks/sql/`, mocked `tests/unit`, e2e off-limits).
- Trigger: maintainer applies the `engineer-bot` label to an issue (author) or a
  PR (followup). Canonical label-gated `if:` copied verbatim from the engine.

### Secrets (all provisioned)
Repo-level: `REVIEW_BOT_APP_ID`, `REVIEW_BOT_APP_PRIVATE_KEY`,
`ENGINEER_BOT_APP_ID`, `ENGINEER_BOT_APP_PRIVATE_KEY`, `DATABRICKS_HOST`,
`DATABRICKS_TOKEN` (also mirrored in the `azure-prod` environment). No
`BOT_ENGINE_PAT` — App-auth replaces it.

### Hard prerequisite (engine-repo admin)
The review-bot and engineer-bot Apps must be **installed on
`databricks-bot-engine` with `contents: read`**, or App-auth install fails for
both bots. Not verifiable/doable from this repo.

## Friction findings (feedback to the engine — the point of being first consumer)

1. **REF mode installs only the engine, not the consumer's product deps.** A
   self-testing repo must add its own `poetry install` (driver repos add
   `dotnet restore`) so the agent can run tests to self-verify. No engine input
   or documented step for "prepare product env" in the non-driver case.
2. **Consumer examples are all driver-repo shaped** (cross-repo `internal-repo`
   checkout, `prepare-driver-env`, dual tokens, SEA configs). A repo that tests
   *itself* has no example to copy; the closest correct template is the engine
   dogfood, which uses LOCAL install (not REF) — so it can't be copied verbatim.
   → Suggest a self-testing consumer example.
3. **Reusable-workflow callers can't read environment-scoped secrets** (a
   `uses:`-only job can't declare `environment:`). The reviewer needed
   `DATABRICKS_HOST`/`DATABRICKS_TOKEN` at repo level; the engineer (own job)
   reads them from `azure-prod` fine. Not documented in the onboarding guide.
4. **Reusable reviewer required a PAT** to install the internal engine (a
   `uses:`-only caller can't mint/inject a token). → Filed as engine issue #97,
   fixed by engine PR #100 (`engine-auth: app`); this consumer uses it.

## Verification status
- All workflow YAML + `.bot/config.yaml` parse. `.bot/` tree complete.
- End-to-end NOT yet exercised: gated on the App-on-engine-repo installation
  (admin). First live signals: a test PR (reviewer) and a labelled throwaway
  issue (engineer author). To be driven once the App installation is confirmed;
  outcomes reported honestly rather than assumed.

## Update (2026-07-15b): reviewer rebuilt on composite actions

**Finding (blocker discovered in live CI):** the reviewer's original design —
a thin caller of the engine's reusable WORKFLOW via `jobs.review.uses:
databricks/databricks-bot-engine/.github/workflows/reviewer-bot.reusable.yml@<sha>`
— fails at parse time with **"error parsing called workflow ... : workflow was
not found."** The reusable file exists at the pinned SHA and the engine's Actions
access is `organization`, but cross-repo resolution of a **reusable workflow** in
a private/internal repo does not succeed from this repo. No consumer uses this
pattern: `databricks-driver-test` and `adbc-drivers/databricks` both call the
engine via **composite actions** in their own jobs, and driver-test's engineer
bot (`uses: databricks/databricks-bot-engine/.github/actions/bot-run@<sha>`) runs
in production — proving cross-repo **composite actions** DO resolve in-org.

**Fix:** rebuilt `reviewer-bot.yml` + `reviewer-bot-followup.yml` as own-job
composite callers (matching the engineer bot + driver-test): checkout →
`bot-setup@<sha>` → mint engine-scoped App token → `install-bot-engine@<sha>`
(App-auth, PAT-free) → `python -m databricks_bot_engine.reviewer_bot.run_review`
/ `.followup`. The security floor (fork gate, open-PR check), input resolution,
and base-commit fetch are replicated inline from the reusable. `environment:
azure-prod` is now declarable (own job) — which also resolves friction #3 (a
reusable-workflow caller couldn't read env-scoped secrets).

**Engine feedback:** the documented "thin reusable-workflow caller" onboarding
path for the reviewer does not work cross-repo. Either the engine must make its
reusable workflows cross-repo resolvable, or the docs should steer consumers to
the composite-action pattern (as used by every working consumer). Filed
separately as an engine issue.

All four bot workflows now use the SAME mechanism — own job + cross-repo
composite actions + engine-scoped App token — so the onboarding is internally
consistent and uses only the resolution path proven to work.

## Update (2026-07-15c): CORRECTED root cause — no cross-repo `uses:` of the engine

The 2026-07-15b "composite actions resolve cross-repo" claim was **wrong**.
Live CI on the rebuilt (composite) reviewer failed at "Set up job" with:

> **Unable to resolve action `databricks/databricks-bot-engine`, not found**

So **neither** cross-repo mechanism works from this repo: not the reusable
workflow ("workflow was not found"), not composite actions ("action not found").
The real constraint: **an external consumer cannot `uses:` the internal engine
repo's actions OR reusable workflows at all.** driver-test only appeared to work
because its reviewer uses a *local* (`./`) composite; adbc's workflows state it
outright — *"a cross-repo `uses:` of the hub's internal composite action does
NOT work."* (It is NOT public-vs-internal: adbc is public too and works, because
it doesn't `uses:` the engine either.)

### Correct pattern (matches adbc, the working external consumer)
Do NOT `uses:` any engine action/workflow. Install the engine with a plain
`pip install "databricks-bot-engine @ git+https://…@github.com/…@<sha>"` in a
`run:` step (an external repo CAN token-clone a private repo), and inline the
setup/dispatch. Implemented here as a **local composite**
`.github/actions/install-bot-engine` (local `./` refs always resolve) that all
four workflows call; it inlines the engine's REF-mode install (JFrog mirror,
git-auth masking, SDK/CLI pins). PAT-free preserved: the engine-scoped App token
is passed to the local composite and used in the git auth header (not a stored
`BOT_ENGINE_PAT`).

All four bot workflows now: mint tokens inline → `./.github/actions/install-bot-engine`
→ run the engine entrypoint via `python -m databricks_bot_engine.*`. Zero
cross-repo `uses:` of the engine.

### Engine feedback
The README onboarding guide (step 2) tells consumers to `jobs.<id>.uses:` the
engine's reusable workflow — which fails for any external consumer. Filed as
engine issue #104; README doc-fix PR to follow. The reusable workflows +
`.github/actions/*` composites are effectively **engine-dogfood-only**.

## Update (2026-07-15d): both bots verified live ✅

**Reviewer** — ran on PR #862 and posted a real review as `peco-review-bot[bot]`
(COMMENTED, inline diff-anchored findings). Confirms: local-composite engine
install, review-bot App token on this repo, engine-scoped App token, PAT-free
`pip install` of the private engine, and the reviewer agent posting.

**Engineer (author)** — verified via a temporary `pull_request`-triggered clone
(`engineer-bot-e2e.yml`, since issues/dispatch triggers only register from main),
author-only against throwaway issue #864. The agent ran 41 turns, loaded
`.bot/config.yaml` + prompts, surveyed `src/databricks/sql/`, and emitted a
correct structured `no_change_needed` outcome. Temp workflow removed + issue
closed after verification.

**Env-prep fix found during verification:** the hand-rolled `poetry install`
failed on the protected runner — the repo's pre-existing stale `poetry.lock`
(pyproject drift) plus egress-block (relock couldn't reach pypi.org). Fixed by
using the repo's own `./.github/actions/setup-poetry` (poetry via the internal
JFrog mirror + relock), the maintained egress-safe path. This is another
first-consumer finding: a self-testing consumer needs its product-dep install to
go through the same internal mirror the engine install uses; `pip`'s
`PIP_INDEX_URL` does not carry to poetry.

**Engineer followup** — same pattern as author (setup-poetry + local install +
inlined dispatch), not independently live-run; author verification + the shared
infra cover its mechanics. Real trigger path (label on a PR) is live after merge.

## Update (2026-07-15e): shared bot-prelude composite (DRY)

All four workflows shared ~4 boilerplate steps (mint bot token, mint
engine-scoped token, Setup Node, install engine). Extracted into a local
`./.github/actions/bot-prelude` composite: inputs `app-id`/`private-key`
(review-bot or engineer-bot) + `engine-ref`; output `token` (the this-repo bot
token). Each workflow now: checkout (own variant) → Setup Python **or**
setup-poetry (the one genuine reviewer-vs-engineer difference — reviewer reads
code so needs only an interpreter; engineer runs the connector's tests so needs
its deps) → `uses: ./.github/actions/bot-prelude` → run tail. bot-prelude calls
the existing local `install-bot-engine`.

Not shareable (kept inline, by design): checkout (differs per flow + a composite
can't run its own loader's checkout), Python-vs-Poetry, and the run/publish tail.
This reviewer-vs-engineer env split is universal across consumers; the specific
dep tool (poetry here, `dotnet restore` for driver-test) is repo-specific — so
the product-dep step stays consumer-owned.

Engine feedback (added to #104): consumers must also duplicate `install-bot-engine`
locally (can't `uses:` the engine's copy cross-repo); sharing it needs the engine
public or the action published separately.
