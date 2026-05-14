"""E2E tests for ``use_sea=True`` (routes through the Rust kernel
via the PyO3 ``databricks_sql_kernel`` module).

PAT auth only. Anything else surfaces as ``NotSupportedError``
from the auth bridge — covered as a unit test, not exercised here.

Skipped automatically when:
  - The standard ``DATABRICKS_SERVER_HOSTNAME`` / ``HTTP_PATH`` /
    ``TOKEN`` creds aren't set (existing connector convention).
  - ``databricks_sql_kernel`` isn't importable (the wheel hasn't
    been installed; run ``pip install
    'databricks-sql-connector[kernel]'`` or, for local dev,
    ``cd databricks-sql-kernel/pyo3 && maturin develop --release``
    into this venv).

Run from the connector repo root:

    set -a && source ~/.databricks/pecotesting-creds && set +a
    .venv/bin/pytest tests/e2e/test_kernel_backend.py -v
"""

from __future__ import annotations

import pytest

import databricks.sql as sql
from databricks.sql.exc import DatabaseError


# Skip the whole module unless the kernel wheel is importable.
pytest.importorskip(
    "databricks_sql_kernel",
    reason="use_sea=True requires the databricks-sql-kernel package",
)


@pytest.fixture(scope="module")
def kernel_conn_params(connection_details):
    """Live-cred check + connection params for use_sea=True.

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
        "use_sea": True,
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


def test_connect_with_use_sea_opens_a_session(conn):
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
    10000 rows. Exercises the CloudFetch / multi-batch path on the
    kernel side."""
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
