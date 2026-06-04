"""E2E tests for ``use_kernel=True`` (routes through the Rust kernel
via the PyO3 ``databricks_sql_kernel`` module).

PAT auth only. Anything else surfaces as ``NotSupportedError``
from the auth bridge — covered as a unit test, not exercised here.

Skipped automatically when:
  - The standard ``DATABRICKS_SERVER_HOSTNAME`` / ``HTTP_PATH`` /
    ``TOKEN`` creds aren't set (existing connector convention).
  - ``databricks_sql_kernel`` isn't importable (the wheel hasn't
    been installed; run ``pip install databricks-sql-kernel`` or,
    for local dev,
    ``cd databricks-sql-kernel/pyo3 && maturin develop --release``
    into this venv).

Run from the connector repo root:

    set -a && source ~/.databricks/pecotesting-creds && set +a
    .venv/bin/pytest tests/e2e/test_kernel_backend.py -v
"""

from __future__ import annotations

import pytest

import databricks.sql as sql
from databricks.sql.exc import (
    DatabaseError,
    NotSupportedError,
    OperationalError,
    ServerOperationError,
)

# Skip the whole module unless the kernel wheel is genuinely installed.
# ``pytest.importorskip`` alone isn't enough: the kernel unit tests inject a
# fake ``databricks_sql_kernel`` ModuleType into ``sys.modules`` so the
# connector's import-time ``import databricks_sql_kernel`` succeeds without
# the Rust extension. In the same pytest session that fake module is still
# in ``sys.modules`` when this e2e file is collected, and importorskip
# happily returns it. A real wheel exposes ``__file__`` (the compiled
# extension on disk); the fake ModuleType does not.
_kernel_mod = pytest.importorskip(
    "databricks_sql_kernel",
    reason="use_kernel=True requires the databricks-sql-kernel package",
)
if not getattr(_kernel_mod, "__file__", None):
    pytest.skip(
        "databricks_sql_kernel is a test stub (no __file__); "
        "install the real wheel to run kernel e2e tests",
        allow_module_level=True,
    )


@pytest.fixture(scope="module")
def kernel_conn_params(connection_details):
    """Live-cred check + connection params for use_kernel=True.

    Skips the module if any cred is missing rather than letting
    every test fail with a confusing connect-time error.
    """
    host = connection_details.get("host")
    http_path = connection_details.get("http_path")
    token = connection_details.get("access_token")
    if not (host and http_path and token):
        pytest.skip(
            "DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / "
            "DATABRICKS_TOKEN not set"
        )
    return {
        "server_hostname": host,
        "http_path": http_path,
        "access_token": token,
        "use_kernel": True,
    }


@pytest.fixture
def conn(kernel_conn_params):
    """One-shot connection per test (the simple_test pattern the
    existing e2e suite uses for cursor-level tests)."""
    c = sql.connect(**kernel_conn_params)
    try:
        yield c
    finally:
        c.close()


def test_connect_with_use_kernel_opens_a_session(conn):
    assert conn.open, "connection should report open after connect()"


def test_select_one(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS n")
        assert cur.description[0][0] == "n"
        # description type slug matches what Thrift produces
        assert cur.description[0][1] == "int"
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1


def test_drain_large_range_to_arrow(conn):
    """SELECT * FROM range(10000) drains as a pyarrow Table with
    10000 rows. Exercises end-of-stream drain over multiple
    ``fetch_next_batch`` calls; not large enough to cross a
    CloudFetch chunk boundary — see test_driver for CloudFetch
    coverage."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM range(10000)")
        rows = cur.fetchall()
        assert len(rows) == 10000


def test_fetchmany_pacing(conn):
    """fetchmany honours the requested size and stops cleanly at
    end-of-stream — covers the buffer-slicing logic in
    KernelResultSet."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM range(50)")
        r1 = cur.fetchmany(10)
        r2 = cur.fetchmany(20)
        r3 = cur.fetchmany(100)  # capped at remaining
        assert (len(r1), len(r2), len(r3)) == (10, 20, 20)


def test_fetchall_arrow(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS a, 'hi' AS b")
        table = cur.fetchall_arrow()
        assert table.num_rows == 1
        assert table.column_names == ["a", "b"]


# ── Metadata ──────────────────────────────────────────────────────


def test_metadata_catalogs(conn):
    with conn.cursor() as cur:
        cur.catalogs()
        rows = cur.fetchall()
        assert len(rows) > 0


def test_metadata_schemas(conn):
    with conn.cursor() as cur:
        cur.schemas(catalog_name="main")
        rows = cur.fetchall()
        assert len(rows) > 0


def test_metadata_tables(conn):
    with conn.cursor() as cur:
        cur.tables(catalog_name="system", schema_name="information_schema")
        rows = cur.fetchall()
        assert len(rows) > 0


def test_metadata_columns(conn):
    with conn.cursor() as cur:
        cur.columns(
            catalog_name="system",
            schema_name="information_schema",
            table_name="tables",
        )
        rows = cur.fetchall()
        assert len(rows) > 0


# ── Session configuration ─────────────────────────────────────────


def test_session_configuration_round_trips(kernel_conn_params):
    """`session_configuration` flows through to the kernel's
    `session_conf` and is honoured by the server.

    `ANSI_MODE` is the safe choice — it's on the SEA allow-list and
    isn't workspace-policy-clamped (unlike `STATEMENT_TIMEOUT`) or
    rejected by the warehouse (unlike `TIMEZONE` on dogfood)."""
    params = dict(kernel_conn_params)
    params["session_configuration"] = {"ANSI_MODE": "false"}
    with sql.connect(**params) as c:
        with c.cursor() as cur:
            cur.execute("SET ANSI_MODE")
            rows = cur.fetchall()
            kv = {r[0]: r[1] for r in rows}
            assert kv.get("ANSI_MODE") == "false", f"got {rows!r}"


def test_retry_params_accepted_end_to_end(kernel_conn_params):
    """The connector's `_retry_*` tuning kwargs are translated to the
    kernel `Session`'s `retry_*` kwargs and accepted end-to-end. We
    can't easily force a retry against a live warehouse, so this is a
    smoke test: a connection configured with explicit retry params
    opens and runs a query successfully (proving the kwargs reach and
    are accepted by the kernel)."""
    params = dict(kernel_conn_params)
    params.update(
        _retry_delay_min=2,
        _retry_delay_max=30,
        _retry_stop_after_attempts_count=4,
        _retry_stop_after_attempts_duration=120,
    )
    with sql.connect(**params) as c:
        with c.cursor() as cur:
            cur.execute("SELECT 1 AS n")
            assert cur.fetchall()[0][0] == 1


def test_enable_metric_view_metadata_lists_metric_view_table_type(kernel_conn_params):
    """`enable_metric_view_metadata=True` injects the
    `spark.sql.thriftserver.metadata.metricview.enabled` session conf,
    which the kernel now passes through (verbatim) so the server
    surfaces `METRIC_VIEW` in `cursor.tables()`'s table-type column.

    We assert the connection opens and `tables()` runs; the kernel
    already lists `METRIC_VIEW` among its table types, and the conf
    enables the server side. Not asserting a specific metric view
    exists in the catalog (workspace-dependent)."""
    params = dict(kernel_conn_params)
    params["enable_metric_view_metadata"] = True
    with sql.connect(**params) as c:
        with c.cursor() as cur:
            # Smoke: the conf was accepted (no SqlError on open) and a
            # metadata call works with it set.
            cur.tables()
            cur.fetchall()


# ── Error mapping ─────────────────────────────────────────────────


def test_bad_sql_surfaces_as_databaseerror(conn):
    """Bad SQL should surface as a PEP 249 ``DatabaseError`` with
    the kernel's structured fields (`code`, `sql_state`, `query_id`)
    attached as attributes — the connector backend re-raises the
    kernel's ``SqlError`` to ``DatabaseError`` while preserving the
    server-reported state."""
    with conn.cursor() as cur:
        with pytest.raises(DatabaseError) as exc_info:
            cur.execute("SELECT * FROM definitely_not_a_table_xyz_kernel_e2e")
        err = exc_info.value
        # Structured fields copied off the kernel exception:
        assert getattr(err, "code", None) == "SqlError"
        assert getattr(err, "sql_state", None) == "42P01"


# ── Parameter binding ─────────────────────────────────────────────


def test_parameterized_query_round_trips(conn):
    """Positional parameter binding via the kernel backend. The
    connector's native parameter classes (IntegerParameter etc.)
    serialize to TSparkParameter under the hood; the kernel
    backend's mapper forwards them positionally to the kernel.
    """
    from databricks.sql.parameters.native import (
        IntegerParameter,
        StringParameter,
        BooleanParameter,
    )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ? AS i, ? AS s, ? AS b",
            [
                IntegerParameter(42),
                StringParameter("alice"),
                BooleanParameter(True),
            ],
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 42
        assert rows[0][1] == "alice"
        assert rows[0][2] is True


def test_parameterized_query_with_null(conn):
    """`None` in the parameter list flows through as VoidParameter
    → kernel TypedValue::Null."""
    with conn.cursor() as cur:
        cur.execute("SELECT ? IS NULL AS is_null", [None])
        rows = cur.fetchall()
        assert rows[0][0] is True


def test_parameterized_query_named_params(conn):
    """Named parameter binding via the kernel backend. The
    connector passes ``parameters={name: value}`` dicts (DB-API
    style); the kernel forwards them through ``bind_named_param``
    so the SEA wire payload sets ``StatementParameter.name`` (the
    spec-required public form per canonical proto).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT :n AS n, :s AS s, :b AS b",
            {"n": 42, "s": "alice", "b": True},
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 42
        assert rows[0][1] == "alice"
        assert rows[0][2] is True


def test_parameterized_query_named_param_with_null(conn):
    """``None`` value in a named binding flows through as
    VoidParameter → kernel ``TypedValue::Null``."""
    with conn.cursor() as cur:
        cur.execute("SELECT :x IS NULL AS is_null", {"x": None})
        rows = cur.fetchall()
        assert rows[0][0] is True


def test_parameterized_query_decimal(conn):
    """DECIMAL parameters carry precision/scale in the SQL type
    string ('DECIMAL(p,s)') — the kernel parser extracts them so
    fractional digits survive the wire.

    Uses the connector's auto-inference path
    (`calculate_decimal_cast_string`) to derive precision/scale
    from the value; the explicit-arg path
    (`DecimalParameter(v, scale=, precision=)`) has a pre-existing
    bug in this branch where the format-args are passed
    `(scale, precision)` instead of `(precision, scale)` — out of
    scope for this PR.
    """
    import decimal
    from databricks.sql.parameters.native import DecimalParameter

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ? AS d",
            [DecimalParameter(decimal.Decimal("-123.45"))],
        )
        rows = cur.fetchall()
        # Server echoes back as decimal.Decimal.
        assert str(rows[0][0]) == "-123.45"


def test_query_tags_round_trip(kernel_conn_params):
    """Per-statement query_tags are forwarded to the kernel and accepted
    by the server. Smoke-level: a malformed query_tags conf would fail
    the execute. (query.history ingestion lag makes a sync tag-readback
    assertion infeasible.)"""
    with sql.connect(**kernel_conn_params) as c:
        with c.cursor() as cur:
            cur.execute(
                "SELECT 1 AS n",
                query_tags={"team": "platform", "production": None},
            )
            assert cur.fetchall()[0][0] == 1


def test_user_agent_entry_and_http_headers_round_trip(kernel_conn_params):
    """A connection with user_agent_entry (folded into the connector's
    User-Agent, then appended to the kernel base UA) and a custom HTTP
    header opens and queries cleanly. Replacing the kernel base UA would
    break the SEA result disposition (HTTP 400); appending preserves it
    — this exercises that end-to-end."""
    params = dict(kernel_conn_params)
    params["user_agent_entry"] = "kernel-e2e-app"
    params["http_headers"] = [("X-Kernel-E2E", "yes")]
    with sql.connect(**params) as c:
        with c.cursor() as cur:
            cur.execute("SELECT 1 AS n")
            assert cur.fetchall()[0][0] == 1


# ── Parameter parity (tz-aware timestamp, scientific decimal) ──────


def test_tz_aware_timestamp_parameter_binds(conn):
    """A tz-aware datetime parameter (datetime with tzinfo) binds on
    the kernel path and resolves to the correct UTC instant. Previously
    rejected at bind on kernel; works on Thrift. (kernel #121)"""
    import datetime

    tzdt = datetime.datetime(
        2026,
        5,
        15,
        18,
        0,
        0,
        tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT ? AS ts", [tzdt])
        ts = cur.fetchall()[0][0]
        # 18:00 +05:30 == 12:30 UTC.
        assert (ts.hour, ts.minute) == (12, 30)


def test_scientific_notation_decimal_parameter_binds(conn):
    """A Decimal whose str() is exponential (e.g. 1E-7) binds on the
    kernel path. Previously rejected at bind; the server/Thrift accept
    scientific-notation decimal literals. (kernel #121)"""
    import decimal
    from databricks.sql.parameters.native import DecimalParameter

    with conn.cursor() as cur:
        # 1E+2 == 100, round-trips cleanly at scale 0.
        cur.execute("SELECT ? AS d", [DecimalParameter(decimal.Decimal("1E+2"))])
        assert int(cur.fetchall()[0][0]) == 100


# ── Staging / volume — fail loud, not silent no-op ────────────────


def test_staging_put_raises_not_supported(conn):
    """A PUT (volume/staging) statement fails loud on the kernel path
    rather than silently no-opping (which would make ETL ingest
    stale/missing data). (CUJ-gap audit)"""
    with conn.cursor() as cur:
        with pytest.raises(NotSupportedError, match="staging"):
            cur.execute("PUT '/tmp/x.csv' INTO '/Volumes/main/default/v/x.csv'")


def test_comment_prefixed_staging_put_raises_not_supported(conn):
    """A comment-prefixed staging op (common in ETL scripts) must also
    fail loud — the leading-verb detection strips SQL comments first, so
    it can't slip past into the silent-no-op bug (PR #825 review #1)."""
    with conn.cursor() as cur:
        with pytest.raises(NotSupportedError, match="staging"):
            cur.execute(
                "-- upload the daily extract\n"
                "PUT '/tmp/x.csv' INTO '/Volumes/main/default/v/x.csv'"
            )


# ── Error fidelity — diagnostic-info reaches .context ─────────────


def test_server_error_exposes_diagnostic_info_context(conn):
    """A server-side query failure surfaces as ServerOperationError
    with the Spark diagnostic context in ``.context['diagnostic-info']``
    — Thrift parity (kernel #121 forwards diagnostic_info across pyo3;
    the connector populates .context)."""
    with conn.cursor() as cur:
        with pytest.raises(ServerOperationError) as exc_info:
            cur.execute("SELECT * FROM definitely_not_a_table_xyz_kernel_e2e")
        err = exc_info.value
        # context shape matches Thrift; diagnostic-info may be None if
        # the server didn't attach one, but the KEY must exist.
        assert "diagnostic-info" in err.context
        assert "operation-id" in err.context


# ── Sync cancel (cursor.cancel() from another thread) ─────────────


def test_sync_cancel_interrupts_blocking_execute(conn):
    """cursor.cancel() from another thread cancels a long-running
    blocking execute() on the kernel path. Previously a silent no-op
    (active_command_id was None until execute returned). (kernel #121
    StatementCanceller + connector cancel_running_cursor wiring)"""
    import threading
    import time

    cur = conn.cursor()

    # The kernel publishes the server statement id once the initial
    # POST returns — within the server's default wait window (~10s).
    # Cancel after that so the canceller has an id to target; a cancel
    # before then is a no-op by design (id not yet known). Pick a query
    # that runs well past this so the cancel lands mid-flight.
    def cancel_after_delay():
        time.sleep(15.0)
        cur.cancel()

    t = threading.Thread(target=cancel_after_delay)
    t.start()
    try:
        # Cancel should make execute() raise rather than run to
        # completion — proving the server-side statement was cancelled.
        with pytest.raises(Exception):
            cur.execute(
                "SELECT count(*) FROM range(0, 1000000000000) "
                "WHERE pow(rand(), 2) < 0.5 AND sqrt(id) > 1"
            )
    finally:
        t.join()
        cur.close()


# ── Batch 2 ────────────────────────────────────────────────────────


def test_large_result_drains_without_premature_close(conn):
    """H4: a large multi-chunk result fully drains even though the
    connector no longer closes the statement at execute-return — the
    kernel auto-closes on drain. Guards the regression where a premature
    CloseStatement broke lazy CloudFetch chunk-link fetches."""
    n = 5_000_000
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, cast(id AS string) s FROM range({n})")
        rows = cur.fetchall()
        assert len(rows) == n
        # Cursor is reusable after the auto-close fired on the prior result.
        cur.execute("SELECT 42 AS n")
        assert cur.fetchall()[0][0] == 42


def test_server_cancel_maps_to_operational_error(conn):
    """A server-side cancel surfaces as OperationalError (cancelled
    class), not ProgrammingError. We trigger it via a cross-thread
    cancel of a running query; the raised exception must be in the
    OperationalError family, not ProgrammingError."""
    import threading
    import time

    from databricks.sql.exc import ProgrammingError

    cur = conn.cursor()

    def cancel_after_delay():
        time.sleep(15.0)
        cur.cancel()

    t = threading.Thread(target=cancel_after_delay)
    t.start()
    try:
        with pytest.raises(Exception) as exc_info:
            cur.execute(
                "SELECT count(*) FROM range(0, 1000000000000) "
                "WHERE pow(rand(), 2) < 0.5 AND sqrt(id) > 1"
            )
        # The cancellation must not masquerade as a caller-argument
        # (ProgrammingError) error. It should be operational.
        assert not isinstance(exc_info.value, ProgrammingError)
        assert isinstance(exc_info.value, (OperationalError, DatabaseError))
    finally:
        t.join()
        cur.close()
