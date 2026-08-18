# Engineer-bot learning log

Durable, reusable engineering learnings distilled by the daily retrospective
(`engineer-bot-learning.yml` → `databricks_bot_engine.engineer_bot.retrospective`).
Each daily run appends one dated section of 0..N learnings to a single rolling PR
until a human merges it. The engineer author phase reads this log (see
`.bot/config.yaml` `author.knowledge_log`) so past lessons steer future fixes.

## Entries

### 2026-08-13: learnings since 2026-08-04T18:01:30Z
- **Context:** PR #905/#906 repaired an HTTP 400 in the retrospective/bot GitHub Actions cron by fixing the `MODEL_ENDPOINT` value.
  **Rule:** In bot workflow YAML, set `MODEL_ENDPOINT` to the concrete `https://<host>/serving-endpoints/<model>/invocations` form; do NOT use `.../serving-endpoints/anthropic/invocations`, which hits `sdk_agent.translate_endpoint`'s already-v2 early-return, keeps the trailing `invocations`, and makes the CLI append `/v1/messages` → 400 (unsupported path).
- **Context:** Run #31030595544 fixed TIMESTAMP_NTZ collapsing to 'timestamp' in `cursor.description`; changing `_col_to_description` to emit a new `type_code` would have silently broken the column-based timestamp parser in `utils.py` keyed on `== "timestamp"`.
  **Rule:** When you change the DB-API `type_code` string a column reports, grep for every downstream branch that matches that string (e.g. type conversion/parsing in `utils.py`) and extend it too — a new type_code that no existing branch matches leaves values unparsed, a silent regression.
- **Context:** Run #31030595544 diagnosed that Spark `TIMESTAMP` and `TIMESTAMP_NTZ` both arrive over Thrift as `TTypeId.TIMESTAMP_TYPE`, so the enum name alone collapses them (same pattern PR #560 used for VARIANT).
  **Rule:** Distinct Spark SQL types that share one Thrift `TTypeId` cannot be disambiguated from the enum name; recover the true type by reading the Arrow field's `Spark:DataType:SqlName` metadata override in `_col_to_description`.
- **Context:** Run #31030595544 verified the TIMESTAMP_NTZ fix; the disambiguating `Spark:DataType:SqlName` Arrow metadata is emitted by the live warehouse, not synthesized client-side.
  **Rule:** When a fix depends on server-populated Arrow schema metadata, prove it with an e2e test against the real warehouse (a mocked unit test asserts the metadata you fabricated, not that the server actually emits it); a passing mock is not evidence the wire carries the field.
- **Context:** Run #31030595544's read-only plan phase spent a turn calling `edit_file` (turn 32) before realizing at turn 34 that edit tools are unavailable during planning.
  **Rule:** In the read-only plan phase the structured plan is the only deliverable — do not attempt `edit_file`/write tools there; defer all edits to the implementation phase to avoid wasted turns.

### 2026-08-15: learnings since 2026-08-14T17:40:48Z
- **Context:** PR #910 forwarded `identity_federation_client_id` through all three kernel auth-resolution branches (M2M, PAT, U2M) in `auth_bridge.py`, guarded by a truthy `if federation_client_id:` check; the reviewer flagged that the field was uniquely untested even though every other forwarded kwarg was pinned per-path.
  **Rule:** When a kwarg/field is forwarded identically across N parallel resolution branches, assert it per-branch (plus a negative case for the omitted/empty guard) — a shared behavior tested on only one branch lets a dropped-field regression on the others pass CI silently.

### 2026-08-18: learnings since 2026-08-16T17:31:37Z
- **Context:** PR #916 (kernel `row_limit`) — a reviewer flagged that treating `row_limit=0` as a hard zero-row cap diverges from SEA (which drops `row_limit<=0` and returns all rows); the maintainer resolved it by stating SEA is incomplete and is not a reference implementation.
  **Rule:** When checking cross-backend behavior alignment in this connector, treat Thrift as the authoritative reference; SEA is incomplete/being deprecated and must NOT be used as an alignment target.
- **Context:** PR #914 (kernel OAuth U2M bundle) — reviewers repeatedly caught that forwarding a caller `oauth_redirect_port` without an explicit `oauth_client_id` produces an unregistered redirect URI, because the default `databricks-sql-python` app only registers ports 8020–8024.
  **Rule:** OAuth U2M `oauth_redirect_port` is coupled to `oauth_client_id` — only override the default port range when BOTH are supplied (mirroring Thrift's `get_python_sql_connector_auth_provider`); a bare port must fall back to the app's registered range.
- **Context:** PR #914 saw many review rounds (scopes hardcoded vs. honored, stale-tree confusion) because "exact parity with Thrift" was asserted in the CHANGELOG, docstring, and comments while the code actually diverged (e.g. honoring caller `oauth_scopes`).
  **Rule:** When a change claims cross-backend parity, keep code, tests, comments, docstring, and CHANGELOG mutually consistent — don't assert "exact parity"/"mirrors X exactly" for behavior that intentionally diverges; scope the parity claim to what actually matches.
- **Context:** PR #913 (connection-parameters doc) — many params show ❌ for the kernel backend because `Session._create_backend` forwards only a curated, named subset of `connect()` kwargs to the kernel rather than splatting `**kwargs`; forwarded retry options use `kwargs.get(...)` with no fallback, so the kernel applies its own Rust defaults.
  **Rule:** On the kernel path, a `connect()`/session kwarg is honored only if `Session._create_backend` explicitly forwards it — anything outside that named subset is silently ignored, and options passed as `kwargs.get(...)` without a fallback let the kernel supply its own default rather than the Thrift default.

--- *Add new entries above this line (oldest→newest); newest sections sort to the bottom.* ---
