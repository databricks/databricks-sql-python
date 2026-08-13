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

--- *Add new entries above this line (oldest→newest); newest sections sort to the bottom.* ---
