"""
End-to-end integration tests for Multi-Statement Transaction (MST) APIs.

Tests driver behavior for MST across:
- Basic correctness (commit/rollback/isolation/multi-table)
- API-specific (autocommit, isolation level, error handling)
- Metadata RPCs inside transactions (non-transactional freshness)
- SQL statements blocked by MSTCheckRule (SHOW, DESCRIBE, information_schema)
- Execute variants (executemany)

Parallelisation:
- Each test uses its own unique table (derived from test name) to allow
  parallel execution with pytest-xdist.
- Tests requiring multiple concurrent connections to the same table are
  tagged with xdist_group so the concurrent connections within a single
  test don't conflict with other tests on different workers.

Requirements:
- DBSQL warehouse that supports Multi-Statement Transactions (MST)
- Env vars: DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH,
  DATABRICKS_TOKEN, DATABRICKS_CATALOG, DATABRICKS_SCHEMA
"""

import logging
import os
import re
import uuid

import pytest

import databricks.sql as sql
from databricks.sql.exc import DatabaseError

logger = logging.getLogger(__name__)


def _unique_table_name(request):
    """Derive a unique Delta table name from the test node id."""
    node_id = request.node.name
    sanitized = re.sub(r"[^a-z0-9_]", "_", node_id.lower())
    return f"mst_pysql_{sanitized}"[:80]


def _unique_table_name_raw(suffix):
    """Non-fixture unique table name helper for extra tables within a test."""
    return f"mst_pysql_{suffix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def mst_conn_params(connection_details):
    """Connection parameters with MST enabled."""
    return {
        "server_hostname": connection_details["host"],
        "http_path": connection_details["http_path"],
        "access_token": connection_details.get("access_token"),
        "ignore_transactions": False,
    }


@pytest.fixture
def mst_catalog(connection_details):
    return connection_details.get("catalog") or os.getenv("DATABRICKS_CATALOG") or "main"


@pytest.fixture
def mst_schema(connection_details):
    return connection_details.get("schema") or os.getenv("DATABRICKS_SCHEMA") or "default"


@pytest.fixture
def mst_table(request, mst_conn_params, mst_catalog, mst_schema):
    """Create a fresh Delta table for the test and drop it afterwards.

    Yields (fq_table_name, table_name). The table is unique per test so tests
    can run in parallel without stepping on each other.
    """
    table_name = _unique_table_name(request)
    fq_table = f"{mst_catalog}.{mst_schema}.{table_name}"

    with sql.connect(**mst_conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {fq_table}")
            cursor.execute(
                f"CREATE TABLE {fq_table} (id INT, value STRING) USING DELTA "
                f"TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')"
            )

    yield fq_table, table_name

    try:
        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {fq_table}")
    except Exception as e:
        logger.warning(f"Failed to drop {fq_table}: {e}")


def _get_row_count(mst_conn_params, fq_table):
    """Count rows from a fresh connection (avoids in-txn caching)."""
    with sql.connect(**mst_conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {fq_table}")
            return cursor.fetchone()[0]


def _get_ids(mst_conn_params, fq_table):
    """Return the set of ids from a fresh connection."""
    with sql.connect(**mst_conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT id FROM {fq_table}")
            return {row[0] for row in cursor.fetchall()}


# ==================== A. BASIC CORRECTNESS ====================


class TestMstCorrectness:
    """Core MST correctness: commit, rollback, isolation, multi-table."""

    def test_commit_single_insert(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'committed')")
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 1

    def test_commit_multiple_inserts(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'a')")
                cursor.execute(f"INSERT INTO {fq_table} VALUES (2, 'b')")
                cursor.execute(f"INSERT INTO {fq_table} VALUES (3, 'c')")
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 3

    def test_rollback_single_insert(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'rolled_back')")
            conn.rollback()

        assert _get_row_count(mst_conn_params, fq_table) == 0

    def test_sequential_transactions(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'txn1')")
            conn.commit()

            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (2, 'txn2')")
            conn.rollback()

            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (3, 'txn3')")
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 2

    def test_auto_start_after_commit(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'txn1')")
            conn.commit()

            # Second INSERT auto-starts a new transaction
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (2, 'txn2')")
            conn.rollback()

        assert _get_ids(mst_conn_params, fq_table) == {1}

    def test_auto_start_after_rollback(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'txn1')")
            conn.rollback()

            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (2, 'txn2')")
            conn.commit()

        assert _get_ids(mst_conn_params, fq_table) == {2}

    def test_update_in_transaction(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'original')")

            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"UPDATE {fq_table} SET value = 'updated' WHERE id = 1")
            conn.commit()

        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT value FROM {fq_table} WHERE id = 1")
                assert cursor.fetchone()[0] == "updated"

    def test_delete_in_transaction(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'a')")
                cursor.execute(f"INSERT INTO {fq_table} VALUES (2, 'b')")

            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {fq_table} WHERE id = 1")
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 1

    def test_multi_table_commit(self, mst_conn_params, mst_table, mst_catalog, mst_schema):
        fq_table1, _ = mst_table
        fq_table2 = f"{mst_catalog}.{mst_schema}.{_unique_table_name_raw('multi_commit_t2')}"

        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {fq_table2}")
                cursor.execute(
                    f"CREATE TABLE {fq_table2} (id INT, value STRING) USING DELTA "
                    f"TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')"
                )
            try:
                conn.autocommit = False
                with conn.cursor() as cursor:
                    cursor.execute(f"INSERT INTO {fq_table1} VALUES (1, 't1')")
                    cursor.execute(f"INSERT INTO {fq_table2} VALUES (1, 't2')")
                conn.commit()

                assert _get_row_count(mst_conn_params, fq_table1) == 1
                assert _get_row_count(mst_conn_params, fq_table2) == 1
            finally:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {fq_table2}")

    def test_multi_table_rollback(self, mst_conn_params, mst_table, mst_catalog, mst_schema):
        fq_table1, _ = mst_table
        fq_table2 = f"{mst_catalog}.{mst_schema}.{_unique_table_name_raw('multi_rb_t2')}"

        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {fq_table2}")
                cursor.execute(
                    f"CREATE TABLE {fq_table2} (id INT, value STRING) USING DELTA "
                    f"TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')"
                )
            try:
                conn.autocommit = False
                with conn.cursor() as cursor:
                    cursor.execute(f"INSERT INTO {fq_table1} VALUES (1, 't1')")
                    cursor.execute(f"INSERT INTO {fq_table2} VALUES (1, 't2')")
                conn.rollback()

                assert _get_row_count(mst_conn_params, fq_table1) == 0
                assert _get_row_count(mst_conn_params, fq_table2) == 0
            finally:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {fq_table2}")

    def test_multi_table_atomicity(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'should_rollback')")
                with pytest.raises(Exception):
                    cursor.execute(
                        "INSERT INTO nonexistent_table_xyz_xyz VALUES (1, 'fail')"
                    )
            conn.rollback()

        assert _get_row_count(mst_conn_params, fq_table) == 0

    @pytest.mark.xdist_group(name="mst_repeatable_reads")
    def test_repeatable_reads(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'initial')")

            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT value FROM {fq_table} WHERE id = 1")
                first_read = cursor.fetchone()[0]

            # External connection modifies data
            with sql.connect(**mst_conn_params) as ext_conn:
                with ext_conn.cursor() as ext_cursor:
                    ext_cursor.execute(
                        f"UPDATE {fq_table} SET value = 'modified' WHERE id = 1"
                    )

            # Re-read in same txn — should see original value
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT value FROM {fq_table} WHERE id = 1")
                second_read = cursor.fetchone()[0]

            assert first_read == second_read, "Repeatable read: value should not change"
            conn.rollback()

    @pytest.mark.xdist_group(name="mst_write_conflict")
    def test_write_conflict_single_table(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as setup_conn:
            with setup_conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'initial')")

        conn1 = sql.connect(**mst_conn_params)
        conn2 = sql.connect(**mst_conn_params)
        try:
            conn1.autocommit = False
            conn2.autocommit = False

            with conn1.cursor() as c1:
                c1.execute(f"UPDATE {fq_table} SET value = 'conn1' WHERE id = 1")
            with conn2.cursor() as c2:
                c2.execute(f"UPDATE {fq_table} SET value = 'conn2' WHERE id = 1")

            conn1.commit()
            with pytest.raises(Exception):
                conn2.commit()
        finally:
            try:
                conn1.close()
            except Exception:
                pass
            try:
                conn2.close()
            except Exception:
                pass

    def test_read_only_transaction(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'existing')")

            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {fq_table}")
                assert cursor.fetchone()[0] == 1
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 1

    def test_rollback_after_query_failure(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'before_error')")
                with pytest.raises(Exception):
                    cursor.execute("SELECT * FROM nonexistent_xyz_xyz")
            conn.rollback()

            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (2, 'after_recovery')")
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 1

    def test_multiple_cursors_in_txn(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as c1:
                c1.execute(f"INSERT INTO {fq_table} VALUES (1, 'c1')")
            with conn.cursor() as c2:
                c2.execute(f"INSERT INTO {fq_table} VALUES (2, 'c2')")
            with conn.cursor() as c3:
                c3.execute(f"INSERT INTO {fq_table} VALUES (3, 'c3')")
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 3

    def test_parameterized_insert(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {fq_table} VALUES (%(id)s, %(value)s)",
                    {"id": 1, "value": "parameterized"},
                )
            conn.commit()

        with sql.connect(**mst_conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT value FROM {fq_table} WHERE id = 1")
                assert cursor.fetchone()[0] == "parameterized"

    def test_empty_transaction_rollback(self, mst_conn_params, mst_table):
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            # Rollback with no DML should not raise
            conn.rollback()

    def test_close_connection_implicit_rollback(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        conn = sql.connect(**mst_conn_params)
        conn.autocommit = False
        with conn.cursor() as cursor:
            cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'pending')")
        conn.close()

        assert _get_row_count(mst_conn_params, fq_table) == 0


# ==================== B. API-SPECIFIC TESTS ====================


class TestMstApi:
    """DB-API-specific tests: autocommit, isolation, error handling."""

    def test_default_autocommit_is_true(self, mst_conn_params):
        with sql.connect(**mst_conn_params) as conn:
            assert conn.autocommit is True

    def test_set_autocommit_false(self, mst_conn_params):
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            assert conn.autocommit is False

    def test_commit_without_active_txn_throws(self, mst_conn_params):
        with sql.connect(**mst_conn_params) as conn:
            with pytest.raises(Exception, match=r"NO_ACTIVE_TRANSACTION"):
                conn.commit()

    def test_set_autocommit_during_active_txn_throws(
        self, mst_conn_params, mst_table
    ):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'active_txn')")
            with pytest.raises(Exception):
                conn.autocommit = True
            conn.rollback()

    def test_supported_isolation_level(self, mst_conn_params):
        with sql.connect(**mst_conn_params) as conn:
            conn.set_transaction_isolation("REPEATABLE_READ")
            assert conn.get_transaction_isolation() == "REPEATABLE_READ"

    def test_unsupported_isolation_level_rejected(self, mst_conn_params):
        with sql.connect(**mst_conn_params) as conn:
            for level in ["READ_UNCOMMITTED", "READ_COMMITTED", "SERIALIZABLE"]:
                with pytest.raises(Exception):
                    conn.set_transaction_isolation(level)


# ==================== C. METADATA RPCs ====================


class TestMstMetadata:
    """Thrift metadata RPCs inside active transactions.

    Python's cursor.columns/tables/schemas/catalogs map to Thrift
    Get{Columns,Tables,Schemas,Catalogs} RPCs. The server's MST guard
    rejects these RPCs with a "not supported within a multi-statement
    transaction" error. The rejection happens before reaching the txn,
    so the active transaction itself remains usable (unlike the SQL
    forms in TestMstBlockedSql, which abort the txn).
    """

    def _assert_metadata_rpc_blocked(self, mst_conn_params, fq_table, rpc):
        """Assert the metadata RPC raises inside an active MST.

        The Thrift Get* RPCs are rejected by the MST gateway before reaching
        the transaction, so the txn itself remains usable — only the RPC
        call fails.

        `rpc` is a callable that takes a cursor and invokes the metadata
        RPC under test.
        """
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'before_blocked')")

                with pytest.raises(DatabaseError, match="multi-statement transaction"):
                    rpc(cursor)
            conn.rollback()

    def test_cursor_columns_blocked(
        self, mst_conn_params, mst_table, mst_catalog, mst_schema
    ):
        fq_table, table_name = mst_table
        self._assert_metadata_rpc_blocked(
            mst_conn_params,
            fq_table,
            lambda cursor: cursor.columns(
                catalog_name=mst_catalog,
                schema_name=mst_schema,
                table_name=table_name,
            ),
        )

    def test_cursor_tables_blocked(
        self, mst_conn_params, mst_table, mst_catalog, mst_schema
    ):
        fq_table, table_name = mst_table
        self._assert_metadata_rpc_blocked(
            mst_conn_params,
            fq_table,
            lambda cursor: cursor.tables(
                catalog_name=mst_catalog,
                schema_name=mst_schema,
                table_name=table_name,
            ),
        )

    def test_cursor_schemas_blocked(self, mst_conn_params, mst_table, mst_catalog):
        fq_table, _ = mst_table
        self._assert_metadata_rpc_blocked(
            mst_conn_params,
            fq_table,
            lambda cursor: cursor.schemas(catalog_name=mst_catalog),
        )

    def test_cursor_catalogs_blocked(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        self._assert_metadata_rpc_blocked(
            mst_conn_params,
            fq_table,
            lambda cursor: cursor.catalogs(),
        )


# ==================== D. BLOCKED SQL (MSTCheckRule) ====================


class TestMstBlockedSql:
    """SQL introspection statements inside active transactions.

    The server restricts MST to an allowlist enforced by MSTCheckRule. The
    TRANSACTION_NOT_SUPPORTED.COMMAND error originally advertised only:
    "Only SELECT / INSERT / MERGE / UPDATE / DELETE / DESCRIBE TABLE are supported."

    The server has since broadened the allowlist to include SHOW COLUMNS
    (ShowDeltaTableColumnsCommand), observed on current DBSQL warehouses.

    Blocked (throw + abort txn):
    - SHOW TABLES, SHOW SCHEMAS, SHOW CATALOGS, SHOW FUNCTIONS
    - DESCRIBE QUERY, DESCRIBE TABLE EXTENDED
    - SELECT FROM information_schema
    - Thrift Get{Catalogs,Schemas,Tables,Columns} RPCs (see TestMstMetadata)

    Allowed:
    - DESCRIBE TABLE (basic form)
    - SHOW COLUMNS
    """

    def _assert_blocked_and_txn_aborted(self, mst_conn_params, fq_table, blocked_sql):
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'before_blocked')")

                with pytest.raises(Exception):
                    cursor.execute(blocked_sql)

                with pytest.raises(Exception):
                    cursor.execute(
                        f"INSERT INTO {fq_table} VALUES (2, 'after_blocked')"
                    )
            try:
                conn.rollback()
            except Exception:
                pass

    def _assert_not_blocked(self, mst_conn_params, fq_table, allowed_sql):
        """Assert the SQL succeeds and returns rows inside an active txn."""
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {fq_table} VALUES (1, 'before')")
                cursor.execute(allowed_sql)
                rows = cursor.fetchall()
                assert len(rows) > 0
            conn.rollback()

    def test_show_tables_blocked(self, mst_conn_params, mst_table, mst_catalog, mst_schema):
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params, fq_table, f"SHOW TABLES IN {mst_catalog}.{mst_schema}"
        )

    def test_show_schemas_blocked(self, mst_conn_params, mst_table, mst_catalog):
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params, fq_table, f"SHOW SCHEMAS IN {mst_catalog}"
        )

    def test_show_catalogs_blocked(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params, fq_table, "SHOW CATALOGS"
        )

    def test_show_functions_blocked(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params, fq_table, "SHOW FUNCTIONS"
        )

    def test_describe_table_extended_blocked(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params, fq_table, f"DESCRIBE TABLE EXTENDED {fq_table}"
        )

    def test_information_schema_blocked(self, mst_conn_params, mst_table, mst_catalog):
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params,
            fq_table,
            f"SELECT * FROM {mst_catalog}.information_schema.columns LIMIT 1",
        )

    def test_show_columns_not_blocked(self, mst_conn_params, mst_table):
        """SHOW COLUMNS succeeds in MST — allowed by the server's MSTCheckRule allowlist."""
        fq_table, _ = mst_table
        self._assert_not_blocked(
            mst_conn_params, fq_table, f"SHOW COLUMNS IN {fq_table}"
        )

    def test_describe_query_blocked(self, mst_conn_params, mst_table):
        """DESCRIBE QUERY is blocked in MST (DescribeQueryCommand)."""
        fq_table, _ = mst_table
        self._assert_blocked_and_txn_aborted(
            mst_conn_params,
            fq_table,
            f"DESCRIBE QUERY SELECT * FROM {fq_table}",
        )

    # DESCRIBE TABLE is explicitly listed as an allowed command in the server's
    # TRANSACTION_NOT_SUPPORTED.COMMAND error message:
    # "Only SELECT / INSERT / MERGE / UPDATE / DELETE / DESCRIBE TABLE are supported."
    def test_describe_table_not_blocked(self, mst_conn_params, mst_table):
        """DESCRIBE TABLE succeeds in MST — explicitly allowed by the server."""
        fq_table, _ = mst_table
        self._assert_not_blocked(
            mst_conn_params, fq_table, f"DESCRIBE TABLE {fq_table}"
        )


# ==================== E. EXECUTE VARIANTS ====================


class TestMstExecuteVariants:
    """Execute method variants (executemany) inside MST."""

    def test_executemany_in_txn(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.executemany(
                    f"INSERT INTO {fq_table} VALUES (%(id)s, %(value)s)",
                    [
                        {"id": 1, "value": "a"},
                        {"id": 2, "value": "b"},
                        {"id": 3, "value": "c"},
                    ],
                )
            conn.commit()

        assert _get_row_count(mst_conn_params, fq_table) == 3

    def test_executemany_rollback_in_txn(self, mst_conn_params, mst_table):
        fq_table, _ = mst_table
        with sql.connect(**mst_conn_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.executemany(
                    f"INSERT INTO {fq_table} VALUES (%(id)s, %(value)s)",
                    [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}],
                )
            conn.rollback()

        assert _get_row_count(mst_conn_params, fq_table) == 0
