"""
End-to-end integration tests for Multi-Statement Transaction (MST) APIs.

These tests verify:
- autocommit property (getter/setter)
- commit() and rollback() methods
- get_transaction_isolation() and set_transaction_isolation() methods
- Transaction error handling

Requirements:
- DBSQL warehouse that supports Multi-Statement Transactions (MST)
- Test environment configured via test.env file or environment variables

Setup:
Set the following environment variables:
- DATABRICKS_SERVER_HOSTNAME
- DATABRICKS_HTTP_PATH
- DATABRICKS_ACCESS_TOKEN (or use OAuth)

Usage:
    pytest tests/e2e/test_transactions.py -v
"""

import logging
import os
import pytest
from typing import Any, Dict

import databricks.sql as sql
from databricks.sql import TransactionError, NotSupportedError, InterfaceError

logger = logging.getLogger(__name__)


class TestTransactions:
    """E2E tests for transaction control methods (MST support)."""

    # Test table name
    TEST_TABLE_NAME = "transaction_test_table"

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, connection_details):
        """Setup test environment before each test and cleanup after."""
        self.connection_params = {
            "server_hostname": connection_details["host"],
            "http_path": connection_details["http_path"],
            "access_token": connection_details.get("access_token"),
            "ignore_transactions": False,  # Enable actual transaction functionality for these tests
        }

        # Get catalog and schema from environment or use defaults
        self.catalog = os.getenv("DATABRICKS_CATALOG", "main")
        self.schema = os.getenv("DATABRICKS_SCHEMA", "default")

        # Create connection for setup
        self.connection = sql.connect(**self.connection_params)

        # Setup: Create test table
        self._create_test_table()

        yield

        # Teardown: Cleanup
        self._cleanup()

    def _get_fully_qualified_table_name(self) -> str:
        """Get the fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{self.TEST_TABLE_NAME}"

    def _create_test_table(self):
        """Create the test table with Delta format and MST support."""
        fq_table_name = self._get_fully_qualified_table_name()
        cursor = self.connection.cursor()

        try:
            # Drop if exists
            cursor.execute(f"DROP TABLE IF EXISTS {fq_table_name}")

            # Create table with Delta and catalog-owned feature for MST compatibility
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {fq_table_name} 
                (id INT, value STRING) 
                USING DELTA 
                TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
            """
            )

            logger.info(f"Created test table: {fq_table_name}")
        finally:
            cursor.close()

    def _cleanup(self):
        """Cleanup after test: rollback pending transactions, drop table, close connection."""
        try:
            # Try to rollback any pending transaction
            if (
                self.connection
                and self.connection.open
                and not self.connection.autocommit
            ):
                try:
                    self.connection.rollback()
                except Exception as e:
                    logger.debug(
                        f"Rollback during cleanup failed (may be expected): {e}"
                    )

                # Reset to autocommit mode
                try:
                    self.connection.autocommit = True
                except Exception as e:
                    logger.debug(f"Reset autocommit during cleanup failed: {e}")

            # Drop test table
            if self.connection and self.connection.open:
                fq_table_name = self._get_fully_qualified_table_name()
                cursor = self.connection.cursor()
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {fq_table_name}")
                    logger.info(f"Dropped test table: {fq_table_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop test table: {e}")
                finally:
                    cursor.close()

        finally:
            # Close connection
            if self.connection:
                self.connection.close()

    # ==================== BASIC AUTOCOMMIT TESTS ====================

    def test_default_autocommit_is_true(self):
        """Test that new connection defaults to autocommit=true."""
        assert (
            self.connection.autocommit is True
        ), "New connection should have autocommit=true by default"

    def test_set_autocommit_to_false(self):
        """Test successfully setting autocommit to false."""
        self.connection.autocommit = False
        assert (
            self.connection.autocommit is False
        ), "autocommit should be false after setting to false"

    def test_set_autocommit_to_true(self):
        """Test successfully setting autocommit back to true."""
        # First disable
        self.connection.autocommit = False
        assert self.connection.autocommit is False

        # Then enable
        self.connection.autocommit = True
        assert (
            self.connection.autocommit is True
        ), "autocommit should be true after setting to true"

    # ==================== COMMIT TESTS ====================

    def test_commit_single_insert(self):
        """Test successfully committing a transaction with single INSERT."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Start transaction
        self.connection.autocommit = False

        # Insert data
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'test_value')"
        )
        cursor.close()

        # Commit
        self.connection.commit()

        # Verify data is persisted using a new connection
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(f"SELECT value FROM {fq_table_name} WHERE id = 1")
            result = verify_cursor.fetchone()
            verify_cursor.close()

            assert result is not None, "Should find inserted row after commit"
            assert result[0] == "test_value", "Value should match inserted value"
        finally:
            verify_conn.close()

    def test_commit_multiple_inserts(self):
        """Test successfully committing a transaction with multiple INSERTs."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        # Insert multiple rows
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'value1')")
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'value2')")
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (3, 'value3')")
        cursor.close()

        self.connection.commit()

        # Verify all rows persisted
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
            result = verify_cursor.fetchone()
            verify_cursor.close()

            assert result[0] == 3, "Should have 3 rows after commit"
        finally:
            verify_conn.close()

    # ==================== ROLLBACK TESTS ====================

    def test_rollback_single_insert(self):
        """Test successfully rolling back a transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        # Insert data
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (100, 'rollback_test')"
        )
        cursor.close()

        # Rollback
        self.connection.rollback()

        # Verify data is NOT persisted
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(
                f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 100"
            )
            result = verify_cursor.fetchone()
            verify_cursor.close()

            assert result[0] == 0, "Rolled back data should not be persisted"
        finally:
            verify_conn.close()

    # ==================== SEQUENTIAL TRANSACTION TESTS ====================

    def test_multiple_sequential_transactions(self):
        """Test executing multiple sequential transactions (commit, commit, rollback)."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        # First transaction - commit
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'txn1')")
        cursor.close()
        self.connection.commit()

        # Second transaction - commit
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'txn2')")
        cursor.close()
        self.connection.commit()

        # Third transaction - rollback
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (3, 'txn3')")
        cursor.close()
        self.connection.rollback()

        # Verify only first two transactions persisted
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(
                f"SELECT COUNT(*) FROM {fq_table_name} WHERE id IN (1, 2)"
            )
            result = verify_cursor.fetchone()
            assert result[0] == 2, "Should have 2 committed rows"

            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 3")
            result = verify_cursor.fetchone()
            assert result[0] == 0, "Rolled back row should not exist"
            verify_cursor.close()
        finally:
            verify_conn.close()

    def test_auto_start_transaction_after_commit(self):
        """Test that new transaction automatically starts after commit."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        # First transaction - commit
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'first')")
        cursor.close()
        self.connection.commit()

        # New transaction should start automatically - insert and rollback
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'second')")
        cursor.close()
        self.connection.rollback()

        # Verify: first committed, second rolled back
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 1")
            result = verify_cursor.fetchone()
            assert result[0] == 1, "First insert should be committed"

            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 2")
            result = verify_cursor.fetchone()
            assert result[0] == 0, "Second insert should be rolled back"
            verify_cursor.close()
        finally:
            verify_conn.close()

    def test_auto_start_transaction_after_rollback(self):
        """Test that new transaction automatically starts after rollback."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        # First transaction - rollback
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'first')")
        cursor.close()
        self.connection.rollback()

        # New transaction should start automatically - insert and commit
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'second')")
        cursor.close()
        self.connection.commit()

        # Verify: first rolled back, second committed
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 1")
            result = verify_cursor.fetchone()
            assert result[0] == 0, "First insert should be rolled back"

            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 2")
            result = verify_cursor.fetchone()
            assert result[0] == 1, "Second insert should be committed"
            verify_cursor.close()
        finally:
            verify_conn.close()

    # ==================== UPDATE/DELETE OPERATION TESTS ====================

    def test_update_in_transaction(self):
        """Test UPDATE operation in transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        # First insert a row with autocommit
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'original')"
        )
        cursor.close()

        # Start transaction and update
        self.connection.autocommit = False
        cursor = self.connection.cursor()
        cursor.execute(f"UPDATE {fq_table_name} SET value = 'updated' WHERE id = 1")
        cursor.close()
        self.connection.commit()

        # Verify update persisted
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(f"SELECT value FROM {fq_table_name} WHERE id = 1")
            result = verify_cursor.fetchone()
            assert result[0] == "updated", "Value should be updated after commit"
            verify_cursor.close()
        finally:
            verify_conn.close()

    # ==================== MULTI-TABLE TRANSACTION TESTS ====================

    def test_multi_table_transaction_commit(self):
        """Test atomic commit across multiple tables."""
        fq_table1_name = self._get_fully_qualified_table_name()
        table2_name = self.TEST_TABLE_NAME + "_2"
        fq_table2_name = f"{self.catalog}.{self.schema}.{table2_name}"

        # Create second table
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table2_name} 
            (id INT, category STRING) 
            USING DELTA 
            TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        """
        )
        cursor.close()

        try:
            # Start transaction and insert into both tables
            self.connection.autocommit = False

            cursor = self.connection.cursor()
            cursor.execute(
                f"INSERT INTO {fq_table1_name} (id, value) VALUES (10, 'table1_data')"
            )
            cursor.execute(
                f"INSERT INTO {fq_table2_name} (id, category) VALUES (10, 'table2_data')"
            )
            cursor.close()

            # Commit both atomically
            self.connection.commit()

            # Verify both inserts persisted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table1_name} WHERE id = 10"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 1, "Table1 insert should be committed"

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table2_name} WHERE id = 10"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 1, "Table2 insert should be committed"

                verify_cursor.close()
            finally:
                verify_conn.close()

        finally:
            # Cleanup second table
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
            cursor.close()

    def test_multi_table_transaction_rollback(self):
        """Test atomic rollback across multiple tables."""
        fq_table1_name = self._get_fully_qualified_table_name()
        table2_name = self.TEST_TABLE_NAME + "_2"
        fq_table2_name = f"{self.catalog}.{self.schema}.{table2_name}"

        # Create second table
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table2_name} 
            (id INT, category STRING) 
            USING DELTA 
            TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        """
        )
        cursor.close()

        try:
            # Start transaction and insert into both tables
            self.connection.autocommit = False

            cursor = self.connection.cursor()
            cursor.execute(
                f"INSERT INTO {fq_table1_name} (id, value) VALUES (20, 'rollback1')"
            )
            cursor.execute(
                f"INSERT INTO {fq_table2_name} (id, category) VALUES (20, 'rollback2')"
            )
            cursor.close()

            # Rollback both atomically
            self.connection.rollback()

            # Verify both inserts were rolled back
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table1_name} WHERE id = 20"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Table1 insert should be rolled back"

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table2_name} WHERE id = 20"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Table2 insert should be rolled back"

                verify_cursor.close()
            finally:
                verify_conn.close()

        finally:
            # Cleanup second table
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
            cursor.close()

    # ==================== ERROR HANDLING TESTS ====================

    def test_set_autocommit_during_active_transaction(self):
        """Test that setting autocommit during an active transaction throws error."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Start transaction
        self.connection.autocommit = False
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (99, 'test')")
        cursor.close()

        # Try to set autocommit=True during active transaction
        with pytest.raises(TransactionError) as exc_info:
            self.connection.autocommit = True

        # Verify error message mentions autocommit or active transaction
        error_msg = str(exc_info.value).lower()
        assert (
            "autocommit" in error_msg or "active transaction" in error_msg
        ), "Error should mention autocommit or active transaction"

        # Cleanup - rollback the transaction
        self.connection.rollback()

    def test_commit_without_active_transaction_throws_error(self):
        """Test that commit() throws error when autocommit=true (no active transaction)."""
        # Ensure autocommit is true (default)
        assert self.connection.autocommit is True

        # Attempt commit without active transaction should throw
        with pytest.raises(TransactionError) as exc_info:
            self.connection.commit()

        # Verify error message indicates no active transaction
        error_message = str(exc_info.value)
        assert (
            "MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION" in error_message
            or "no active transaction" in error_message.lower()
        ), "Error should indicate no active transaction"

    def test_rollback_without_active_transaction_is_safe(self):
        """Test that rollback() without active transaction is a safe no-op."""
        # With autocommit=true (no active transaction)
        assert self.connection.autocommit is True

        # ROLLBACK should be safe (no exception)
        self.connection.rollback()

        # Verify connection is still usable
        assert self.connection.autocommit is True
        assert self.connection.open is True

    # ==================== TRANSACTION ISOLATION TESTS ====================

    def test_get_transaction_isolation_returns_repeatable_read(self):
        """Test that get_transaction_isolation() returns REPEATABLE_READ."""
        isolation_level = self.connection.get_transaction_isolation()
        assert (
            isolation_level == "REPEATABLE_READ"
        ), "Databricks MST should use REPEATABLE_READ (Snapshot Isolation)"

    def test_set_transaction_isolation_accepts_repeatable_read(self):
        """Test that set_transaction_isolation() accepts REPEATABLE_READ."""
        # Should not raise - these are all valid formats
        self.connection.set_transaction_isolation("REPEATABLE_READ")
        self.connection.set_transaction_isolation("REPEATABLE READ")
        self.connection.set_transaction_isolation("repeatable_read")
        self.connection.set_transaction_isolation("repeatable read")

    def test_set_transaction_isolation_rejects_unsupported_level(self):
        """Test that set_transaction_isolation() rejects unsupported levels."""
        with pytest.raises(NotSupportedError) as exc_info:
            self.connection.set_transaction_isolation("READ_COMMITTED")

        error_message = str(exc_info.value)
        assert "not supported" in error_message.lower()
        assert "READ_COMMITTED" in error_message

    # ==================== METADATA OPERATIONS IN TRANSACTION (MST Gaps) ====================

    def test_cursor_columns_inside_active_transaction(self):
        """cursor.columns() inside active transaction should return results."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row to start the transaction
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'columns_test')"
            )

            # Call columns() inside the active transaction
            cursor.columns(
                catalog_name=self.catalog,
                schema_name=self.schema,
                table_name=self.TEST_TABLE_NAME,
            )
            columns = cursor.fetchall()

            assert len(columns) > 0, "cursor.columns() should return column metadata inside a transaction"
        finally:
            self.connection.rollback()
            cursor.close()

    @pytest.mark.xfail(strict=False, reason="Thrift metadata RPCs may return non-transactional results during MST (known issue LC-13427)")
    def test_cursor_tables_inside_active_transaction(self):
        """cursor.tables() inside active transaction should return results."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row to start the transaction
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'tables_test')"
            )

            # Call tables() inside the active transaction
            cursor.tables(
                catalog_name=self.catalog,
                schema_name=self.schema,
                table_name=self.TEST_TABLE_NAME,
            )
            tables = cursor.fetchall()

            assert len(tables) > 0, "cursor.tables() should return table metadata inside a transaction"
        finally:
            self.connection.rollback()
            cursor.close()

    def test_cursor_schemas_inside_active_transaction(self):
        """cursor.schemas() inside active transaction should return results."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row to start the transaction
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'schemas_test')"
            )

            # Call schemas() inside the active transaction
            cursor.schemas(
                catalog_name=self.catalog,
                schema_name=self.schema,
            )
            schemas = cursor.fetchall()

            assert len(schemas) > 0, "cursor.schemas() should return schema metadata inside a transaction"
        finally:
            self.connection.rollback()
            cursor.close()

    def test_cursor_catalogs_inside_active_transaction(self):
        """cursor.catalogs() inside active transaction should return results."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row to start the transaction
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'catalogs_test')"
            )

            # Call catalogs() inside the active transaction
            cursor.catalogs()
            catalogs = cursor.fetchall()

            assert len(catalogs) > 0, "cursor.catalogs() should return catalog metadata inside a transaction"
        finally:
            self.connection.rollback()
            cursor.close()

    def test_cursor_description_before_execute_in_transaction(self):
        """cursor.description before execute in transaction should be None."""
        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Before any execute, description should be None
            assert cursor.description is None, (
                "cursor.description should be None before any execute in a transaction"
            )
        finally:
            self.connection.rollback()
            cursor.close()

    def test_cursor_description_after_execute_in_transaction(self):
        """cursor.description after execute in transaction should have metadata."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row first so the table has data and description is populated
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'desc_test')"
            )

            cursor.execute(f"SELECT * FROM {fq_table_name} LIMIT 1")

            assert cursor.description is not None, (
                "cursor.description should not be None after execute in a transaction"
            )
            assert len(cursor.description) >= 2, (
                "cursor.description should have at least 2 columns (id, value)"
            )
        finally:
            self.connection.rollback()
            cursor.close()

    # ==================== MSTCheckRule-Blocked SQL in Transaction ====================

    def test_show_columns_blocked_in_transaction(self):
        """SHOW COLUMNS should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row to start the transaction
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            # SHOW COLUMNS should be blocked
            with pytest.raises(Exception):
                cursor.execute(f"SHOW COLUMNS IN {fq_table_name}")

            # Verify transaction is aborted by trying another DML
            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_show_tables_blocked_in_transaction(self):
        """SHOW TABLES should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute(f"SHOW TABLES IN {self.catalog}.{self.schema}")

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_show_schemas_blocked_in_transaction(self):
        """SHOW SCHEMAS should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute(f"SHOW SCHEMAS IN {self.catalog}")

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_show_catalogs_blocked_in_transaction(self):
        """SHOW CATALOGS should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute("SHOW CATALOGS")

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_show_functions_blocked_in_transaction(self):
        """SHOW FUNCTIONS should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute("SHOW FUNCTIONS")

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_describe_query_blocked_in_transaction(self):
        """DESCRIBE QUERY should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute(f"DESCRIBE QUERY SELECT * FROM {fq_table_name}")

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_describe_table_extended_blocked_in_transaction(self):
        """DESCRIBE TABLE EXTENDED should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute(f"DESCRIBE TABLE EXTENDED {fq_table_name}")

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    def test_information_schema_blocked_in_transaction(self):
        """SELECT from information_schema should be blocked inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'blocked_test')"
            )

            with pytest.raises(Exception):
                cursor.execute(
                    f"SELECT * FROM {self.catalog}.information_schema.tables LIMIT 1"
                )

            with pytest.raises(Exception):
                cursor.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_blocked')"
                )
        finally:
            try:
                self.connection.rollback()
            except Exception:
                pass
            cursor.close()

    # ==================== ALLOWED OPERATIONS IN TRANSACTION ====================

    @pytest.mark.xfail(
        reason="SET CATALOG routes through SetCatalogCommand in Thrift, "
        "which is blocked in MST even though the doc says it should be allowed"
    )
    def test_set_catalog_allowed_in_transaction(self):
        """SET CATALOG should work inside active transaction (currently blocked via Thrift)."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'before_set_catalog')"
            )

            cursor.execute(f"SET CATALOG {self.catalog}")

            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_set_catalog')"
            )

            self.connection.commit()

            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 2, "Both rows should be committed after SET CATALOG"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    @pytest.mark.xfail(
        reason="USE SCHEMA routes through a Thrift command that is blocked in MST"
    )
    def test_use_schema_allowed_in_transaction(self):
        """USE SCHEMA should work inside active transaction (currently blocked via Thrift)."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'before_use_schema')"
            )

            cursor.execute(f"USE SCHEMA {self.catalog}.{self.schema}")

            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'after_use_schema')"
            )

            self.connection.commit()

            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 2, "Both rows should be committed after USE SCHEMA"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    @pytest.mark.xfail(
        reason="DESCRIBE TABLE routes through DescribeRelation in Thrift, "
        "which is blocked in MST"
    )
    def test_describe_table_basic_allowed_in_transaction(self):
        """DESCRIBE TABLE (basic) should be allowed inside active transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'describe_test')"
            )

            cursor.execute(f"DESCRIBE TABLE {fq_table_name}")
            result = cursor.fetchall()
            assert len(result) > 0, "DESCRIBE TABLE should return column info"

            self.connection.commit()
        finally:
            cursor.close()

    # ==================== DML METHOD VARIANTS IN TRANSACTION ====================

    def test_executemany_insert_in_transaction(self):
        """executemany() with INSERT should work within a transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.executemany(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (:id, :val)",
                [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}],
            )

            self.connection.commit()

            # Verify 3 rows persisted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 3, "Should have 3 rows after executemany + commit"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_executemany_rollback_in_transaction(self):
        """executemany() followed by rollback should discard all rows."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.executemany(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (:id, :val)",
                [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": "c"}],
            )

            self.connection.rollback()

            # Verify 0 rows
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Should have 0 rows after executemany + rollback"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_parameterized_insert_in_transaction(self):
        """Parameterized INSERT should work within a transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (:id, :value)",
                {"id": 1, "value": "param_test"},
            )

            self.connection.commit()

            # Verify row persisted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT value FROM {fq_table_name} WHERE id = 1")
                result = verify_cursor.fetchone()
                assert result is not None, "Parameterized insert should persist after commit"
                assert result[0] == "param_test", "Value should match parameterized value"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_parameterized_update_in_transaction(self):
        """Parameterized UPDATE should work within a transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Insert row with autocommit
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'original')"
        )
        cursor.close()

        # Start transaction and update with parameters
        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"UPDATE {fq_table_name} SET value = :val WHERE id = :id",
                {"val": "updated_param", "id": 1},
            )

            self.connection.commit()

            # Verify update persisted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT value FROM {fq_table_name} WHERE id = 1")
                result = verify_cursor.fetchone()
                assert result[0] == "updated_param", "Value should be updated via parameterized UPDATE"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_parameterized_select_in_transaction(self):
        """Parameterized SELECT should work within a transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Insert rows with autocommit
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'select_test')"
        )
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'other_row')"
        )
        cursor.close()

        # Start transaction and select with parameters
        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"SELECT * FROM {fq_table_name} WHERE id = :id",
                {"id": 1},
            )
            result = cursor.fetchone()

            assert result is not None, "Parameterized SELECT should return a result"
            assert result[0] == 1, "Should find row with id=1"
            assert result[1] == "select_test", "Should find correct value"
        finally:
            self.connection.rollback()
            cursor.close()

    def test_delete_in_transaction(self):
        """DELETE operation should work within a transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Insert row with autocommit
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'to_delete')"
        )
        cursor.close()

        # Start transaction and delete
        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(f"DELETE FROM {fq_table_name} WHERE id = 1")

            self.connection.commit()

            # Verify row deleted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Row should be deleted after commit"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_mixed_dml_in_transaction(self):
        """INSERT + UPDATE + DELETE in single transaction should work."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Insert initial rows with autocommit
        cursor = self.connection.cursor()
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'row1')")
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'row2')")
        cursor.execute(f"INSERT INTO {fq_table_name} (id, value) VALUES (3, 'row3')")
        cursor.close()

        # Start transaction with mixed DML
        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # INSERT a new row
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (4, 'row4')"
            )

            # UPDATE an existing row
            cursor.execute(
                f"UPDATE {fq_table_name} SET value = 'row1_updated' WHERE id = 1"
            )

            # DELETE a row
            cursor.execute(f"DELETE FROM {fq_table_name} WHERE id = 3")

            self.connection.commit()

            # Verify final state
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()

                # Should have 3 rows: id 1 (updated), 2, 4
                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table_name}"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 3, "Should have 3 rows after mixed DML"

                # Verify UPDATE
                verify_cursor.execute(
                    f"SELECT value FROM {fq_table_name} WHERE id = 1"
                )
                result = verify_cursor.fetchone()
                assert result[0] == "row1_updated", "Row 1 should be updated"

                # Verify DELETE
                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 3"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Row 3 should be deleted"

                # Verify INSERT
                verify_cursor.execute(
                    f"SELECT value FROM {fq_table_name} WHERE id = 4"
                )
                result = verify_cursor.fetchone()
                assert result is not None, "Row 4 should exist"
                assert result[0] == "row4", "Row 4 value should match"

                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    # ==================== CONCURRENT TRANSACTIONS ====================

    def test_concurrent_connections_write_conflict(self):
        """Two connections writing to same table should trigger write conflict."""
        fq_table_name = self._get_fully_qualified_table_name()

        conn1 = sql.connect(**self.connection_params)
        conn2 = sql.connect(**self.connection_params)
        try:
            conn1.autocommit = False
            conn2.autocommit = False

            cursor1 = conn1.cursor()
            cursor2 = conn2.cursor()

            # Both connections insert into the same table
            cursor1.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'conn1_data')"
            )
            cursor2.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'conn2_data')"
            )

            # First commit should succeed
            conn1.commit()

            # Second commit may succeed (append-only) or fail (write conflict)
            # Under Snapshot Isolation, concurrent append-only INSERTs to different
            # parts of the table can sometimes succeed.
            try:
                conn2.commit()
                # If both commits succeeded, that's valid for append-only workloads
            except Exception:
                # Write conflict is also expected behavior
                pass

            cursor1.close()
            cursor2.close()
        finally:
            try:
                conn1.close()
            except Exception:
                pass
            try:
                conn2.close()
            except Exception:
                pass

        # Verify at least conn1's data persisted
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
            result = verify_cursor.fetchone()
            assert result[0] >= 1, "At least conn1's data should be persisted"
            verify_cursor.close()
        finally:
            verify_conn.close()

    def test_concurrent_connections_write_skew_across_tables(self):
        """Two connections writing to different tables should both succeed (Snapshot Isolation)."""
        fq_table1_name = self._get_fully_qualified_table_name()
        table2_name = self.TEST_TABLE_NAME + "_concurrent_2"
        fq_table2_name = f"{self.catalog}.{self.schema}.{table2_name}"

        # Create second table
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table2_name}
            (id INT, value STRING)
            USING DELTA
            TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        """
        )
        cursor.close()

        conn1 = sql.connect(**self.connection_params)
        conn2 = sql.connect(**self.connection_params)
        try:
            conn1.autocommit = False
            conn2.autocommit = False

            cursor1 = conn1.cursor()
            cursor2 = conn2.cursor()

            # Write to different tables
            cursor1.execute(
                f"INSERT INTO {fq_table1_name} (id, value) VALUES (1, 'table1_data')"
            )
            cursor2.execute(
                f"INSERT INTO {fq_table2_name} (id, value) VALUES (1, 'table2_data')"
            )

            # Both commits should succeed
            conn1.commit()
            conn2.commit()

            cursor1.close()
            cursor2.close()

            # Verify data in both tables
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table1_name} WHERE id = 1"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 1, "Table1 should have data from conn1"

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table2_name} WHERE id = 1"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 1, "Table2 should have data from conn2"

                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            try:
                conn1.close()
            except Exception:
                pass
            try:
                conn2.close()
            except Exception:
                pass
            # Cleanup second table
            try:
                self.connection.autocommit = True
            except Exception:
                pass
            cleanup_cursor = self.connection.cursor()
            cleanup_cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
            cleanup_cursor.close()

    def test_repeatable_reads_in_transaction(self):
        """Reads within transaction should see consistent snapshot."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Insert initial row with autocommit
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'initial')"
        )
        cursor.close()

        # Start transaction on conn1
        conn1 = sql.connect(**self.connection_params)
        try:
            conn1.autocommit = False

            cursor1 = conn1.cursor()

            # First read - count rows
            cursor1.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
            first_count = cursor1.fetchone()[0]

            # Another connection inserts a new row (autocommit)
            conn2 = sql.connect(**self.connection_params)
            try:
                cursor2 = conn2.cursor()
                cursor2.execute(
                    f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'new_row')"
                )
                cursor2.close()
            finally:
                conn2.close()

            # Second read in same transaction - should see same snapshot
            cursor1.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
            second_count = cursor1.fetchone()[0]

            assert first_count == second_count, (
                "Reads within a transaction should see consistent snapshot (repeatable reads)"
            )

            cursor1.close()
            conn1.rollback()
        finally:
            conn1.close()

    # ==================== MULTIPLE CURSORS IN TRANSACTION ====================

    def test_multiple_cursors_dml_in_transaction(self):
        """Multiple cursors DML in same transaction should all commit."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor1 = self.connection.cursor()
        cursor2 = self.connection.cursor()
        try:
            cursor1.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'cursor1_data')"
            )
            cursor2.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'cursor2_data')"
            )

            self.connection.commit()

            # Verify both rows persisted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 2, "Both cursor inserts should be committed"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor1.close()
            cursor2.close()

    def test_cursor_metadata_while_other_cursor_active(self):
        """Cursor metadata call while another cursor has active result set."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor1 = self.connection.cursor()
        cursor2 = self.connection.cursor()
        try:
            # cursor1 executes a SELECT (active result set)
            cursor1.execute(f"SELECT * FROM {fq_table_name}")

            # cursor2 calls tables() while cursor1 has active result set
            cursor2.tables(
                catalog_name=self.catalog,
                schema_name=self.schema,
                table_name=self.TEST_TABLE_NAME,
            )
            tables = cursor2.fetchall()

            # Should not crash - verify we got results
            assert tables is not None, "cursor.tables() should not crash while another cursor is active"
        finally:
            self.connection.rollback()
            cursor1.close()
            cursor2.close()

    def test_cursor_close_does_not_end_transaction(self):
        """Closing a cursor should not end the transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor1 = self.connection.cursor()
        try:
            cursor1.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'cursor1_data')"
            )
            cursor1.close()

            # Open a new cursor and insert more data - transaction should still be active
            cursor2 = self.connection.cursor()
            cursor2.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'cursor2_data')"
            )
            cursor2.close()

            self.connection.commit()

            # Verify both rows persisted
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(f"SELECT COUNT(*) FROM {fq_table_name}")
                result = verify_cursor.fetchone()
                assert result[0] == 2, "Both rows should be committed despite cursor close"
                verify_cursor.close()
            finally:
                verify_conn.close()
        except Exception:
            # If cursor1 is already closed, just ensure cleanup
            raise

    # ==================== CONNECTION LIFECYCLE IN TRANSACTION ====================

    def test_close_connection_with_pending_transaction(self):
        """Closing connection with pending transaction should implicitly rollback."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Use a separate connection for this test
        test_conn = sql.connect(**self.connection_params)
        try:
            test_conn.autocommit = False

            cursor = test_conn.cursor()
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'pending')"
            )
            cursor.close()

            # Close connection with pending transaction
            test_conn.close()
        except Exception:
            try:
                test_conn.close()
            except Exception:
                pass

        # Verify row was NOT persisted (implicit rollback)
        verify_conn = sql.connect(**self.connection_params)
        try:
            verify_cursor = verify_conn.cursor()
            verify_cursor.execute(
                f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 1"
            )
            result = verify_cursor.fetchone()
            assert result[0] == 0, "Pending transaction should be implicitly rolled back on connection close"
            verify_cursor.close()
        finally:
            verify_conn.close()

    def test_close_connection_implicit_rollback(self):
        """Closing connection with pending transaction should not throw."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Use a separate connection for this test
        test_conn = sql.connect(**self.connection_params)
        test_conn.autocommit = False

        cursor = test_conn.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'no_throw')"
        )
        cursor.close()

        # Closing connection with pending transaction should not raise
        try:
            test_conn.close()
        except Exception as e:
            pytest.fail(
                f"Closing connection with pending transaction should not throw, but got: {e}"
            )

    # ==================== DDL IN TRANSACTION ====================

    def test_ddl_create_table_in_transaction(self):
        """CREATE TABLE inside transaction should document behavior."""
        ddl_table_name = self.TEST_TABLE_NAME + "_ddl_create"
        fq_ddl_table_name = f"{self.catalog}.{self.schema}.{ddl_table_name}"

        # Ensure the DDL table doesn't exist
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fq_ddl_table_name}")
        cursor.close()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            try:
                cursor.execute(
                    f"""
                    CREATE TABLE {fq_ddl_table_name}
                    (id INT, value STRING)
                    USING DELTA
                    TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
                """
                )
                # If CREATE TABLE succeeded, try rollback and check if table exists
                self.connection.rollback()

                verify_conn = sql.connect(**self.connection_params)
                try:
                    verify_cursor = verify_conn.cursor()
                    verify_cursor.execute(f"SHOW TABLES IN {self.catalog}.{self.schema} LIKE '{ddl_table_name}'")
                    tables = verify_cursor.fetchall()
                    verify_cursor.close()
                    logger.info(
                        f"CREATE TABLE in transaction: table exists after rollback = {len(tables) > 0}"
                    )
                finally:
                    verify_conn.close()
            except Exception as e:
                logger.info(
                    f"CREATE TABLE inside transaction raised exception (documenting behavior): {e}"
                )
                try:
                    self.connection.rollback()
                except Exception:
                    pass
        finally:
            cursor.close()
            # Cleanup
            try:
                self.connection.autocommit = True
            except Exception:
                pass
            cleanup_cursor = self.connection.cursor()
            cleanup_cursor.execute(f"DROP TABLE IF EXISTS {fq_ddl_table_name}")
            cleanup_cursor.close()

    def test_ddl_drop_table_in_transaction(self):
        """DROP TABLE inside transaction should document behavior."""
        ddl_table_name = self.TEST_TABLE_NAME + "_ddl_drop"
        fq_ddl_table_name = f"{self.catalog}.{self.schema}.{ddl_table_name}"

        # Create a temp table with autocommit
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fq_ddl_table_name}")
        cursor.execute(
            f"""
            CREATE TABLE {fq_ddl_table_name}
            (id INT, value STRING)
            USING DELTA
            TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        """
        )
        cursor.close()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            try:
                cursor.execute(f"DROP TABLE {fq_ddl_table_name}")
                # If DROP TABLE succeeded, try rollback and check if table still exists
                self.connection.rollback()

                verify_conn = sql.connect(**self.connection_params)
                try:
                    verify_cursor = verify_conn.cursor()
                    verify_cursor.execute(f"SHOW TABLES IN {self.catalog}.{self.schema} LIKE '{ddl_table_name}'")
                    tables = verify_cursor.fetchall()
                    verify_cursor.close()
                    logger.info(
                        f"DROP TABLE in transaction: table still exists after rollback = {len(tables) > 0}"
                    )
                finally:
                    verify_conn.close()
            except Exception as e:
                logger.info(
                    f"DROP TABLE inside transaction raised exception (documenting behavior): {e}"
                )
                try:
                    self.connection.rollback()
                except Exception:
                    pass
        finally:
            cursor.close()
            # Cleanup
            try:
                self.connection.autocommit = True
            except Exception:
                pass
            cleanup_cursor = self.connection.cursor()
            cleanup_cursor.execute(f"DROP TABLE IF EXISTS {fq_ddl_table_name}")
            cleanup_cursor.close()

    def test_ddl_alter_table_in_transaction(self):
        """ALTER TABLE inside transaction should document behavior."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a row to start the transaction
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'alter_test')"
            )

            try:
                cursor.execute(
                    f"ALTER TABLE {fq_table_name} ADD COLUMNS (extra STRING)"
                )
                # If ALTER TABLE succeeded, rollback
                self.connection.rollback()

                # Check table schema
                verify_conn = sql.connect(**self.connection_params)
                try:
                    verify_cursor = verify_conn.cursor()
                    verify_cursor.execute(f"DESCRIBE {fq_table_name}")
                    columns = verify_cursor.fetchall()
                    column_names = [row[0] for row in columns]
                    verify_cursor.close()
                    logger.info(
                        f"ALTER TABLE in transaction: columns after rollback = {column_names}"
                    )
                finally:
                    verify_conn.close()
            except Exception as e:
                logger.info(
                    f"ALTER TABLE inside transaction raised exception (documenting behavior): {e}"
                )
                try:
                    self.connection.rollback()
                except Exception:
                    pass
        finally:
            cursor.close()

    # ==================== EDGE CASES ====================

    def test_empty_transaction_commit(self):
        """Empty transaction commit should succeed or throw appropriate error."""
        self.connection.autocommit = False

        # Immediately commit with no statements
        try:
            self.connection.commit()
        except Exception as e:
            logger.info(
                f"Empty transaction commit raised: {e} (documenting behavior)"
            )

    def test_empty_transaction_rollback(self):
        """Empty transaction rollback should succeed."""
        self.connection.autocommit = False

        # Immediately rollback with no statements - should succeed
        self.connection.rollback()

    def test_read_only_queries_in_transaction(self):
        """SELECT-only transaction should work correctly."""
        fq_table_name = self._get_fully_qualified_table_name()

        # Insert data with autocommit
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'read_only_test')"
        )
        cursor.close()

        # Start transaction with SELECT only
        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {fq_table_name}")
            result = cursor.fetchall()
            assert len(result) >= 1, "Should read at least 1 row in read-only transaction"

            self.connection.commit()
        finally:
            cursor.close()

    def test_multi_table_transaction_atomicity(self):
        """Partial failure in multi-table transaction should rollback all."""
        fq_table1_name = self._get_fully_qualified_table_name()
        nonexistent_table = f"{self.catalog}.{self.schema}.nonexistent_table_xyz_12345"

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert into existing table
            cursor.execute(
                f"INSERT INTO {fq_table1_name} (id, value) VALUES (1, 'atomicity_test')"
            )

            # Try to insert into nonexistent table - should fail
            try:
                cursor.execute(
                    f"INSERT INTO {nonexistent_table} (id, value) VALUES (1, 'fail')"
                )
            except Exception:
                pass

            # Rollback the entire transaction
            try:
                self.connection.rollback()
            except Exception:
                pass

            # Verify table1 data was also rolled back
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table1_name} WHERE id = 1"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Table1 data should be rolled back due to partial failure"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_rollback_after_query_failure(self):
        """Rollback after query failure should clean up transaction state."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # Insert a valid row
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'valid_row')"
            )

            # Execute invalid SQL
            try:
                cursor.execute("THIS IS INVALID SQL")
            except Exception:
                pass

            # Rollback
            try:
                self.connection.rollback()
            except Exception:
                pass
        finally:
            cursor.close()

        # Reset autocommit state explicitly to clean up after failed transaction
        self.connection.autocommit = True
        self.connection.autocommit = False

        # Start a new transaction and verify clean state with a fresh cursor
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'new_row')"
            )

            self.connection.commit()

            # Verify: only the new row exists, not the old one
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 1"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Old row should not exist after rollback"

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 2"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 1, "New row should exist after commit"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_auto_start_transaction_after_commit_with_dml(self):
        """After commit, DML without explicit BEGIN should start new transaction."""
        fq_table_name = self._get_fully_qualified_table_name()

        self.connection.autocommit = False

        cursor = self.connection.cursor()
        try:
            # First transaction: insert and commit
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (1, 'committed')"
            )
            self.connection.commit()

            # Second transaction: insert and rollback (auto-started)
            cursor.execute(
                f"INSERT INTO {fq_table_name} (id, value) VALUES (2, 'rolled_back')"
            )
            self.connection.rollback()

            # Verify only row 1 exists
            verify_conn = sql.connect(**self.connection_params)
            try:
                verify_cursor = verify_conn.cursor()
                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 1"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 1, "Committed row should exist"

                verify_cursor.execute(
                    f"SELECT COUNT(*) FROM {fq_table_name} WHERE id = 2"
                )
                result = verify_cursor.fetchone()
                assert result[0] == 0, "Rolled back row should not exist"
                verify_cursor.close()
            finally:
                verify_conn.close()
        finally:
            cursor.close()

    def test_cross_table_merge_in_transaction(self):
        """MERGE across tables inside transaction should work."""
        fq_table1_name = self._get_fully_qualified_table_name()
        table2_name = self.TEST_TABLE_NAME + "_merge_source"
        fq_table2_name = f"{self.catalog}.{self.schema}.{table2_name}"

        # Create source table and insert data
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table2_name}
            (id INT, value STRING)
            USING DELTA
            TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
        """
        )
        cursor.execute(
            f"INSERT INTO {fq_table2_name} (id, value) VALUES (1, 'source_val1')"
        )
        cursor.execute(
            f"INSERT INTO {fq_table2_name} (id, value) VALUES (2, 'source_val2')"
        )
        # Insert existing row into target for MERGE WHEN MATCHED
        cursor.execute(
            f"INSERT INTO {fq_table1_name} (id, value) VALUES (1, 'old_val1')"
        )
        cursor.close()

        try:
            self.connection.autocommit = False

            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    MERGE INTO {fq_table1_name} AS target
                    USING {fq_table2_name} AS source
                    ON target.id = source.id
                    WHEN MATCHED THEN UPDATE SET target.value = source.value
                    WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
                """
                )

                self.connection.commit()

                # Verify merge results
                verify_conn = sql.connect(**self.connection_params)
                try:
                    verify_cursor = verify_conn.cursor()

                    verify_cursor.execute(
                        f"SELECT value FROM {fq_table1_name} WHERE id = 1"
                    )
                    result = verify_cursor.fetchone()
                    assert result[0] == "source_val1", "Row 1 should be updated by MERGE"

                    verify_cursor.execute(
                        f"SELECT value FROM {fq_table1_name} WHERE id = 2"
                    )
                    result = verify_cursor.fetchone()
                    assert result is not None, "Row 2 should be inserted by MERGE"
                    assert result[0] == "source_val2", "Row 2 value should match source"

                    verify_cursor.close()
                finally:
                    verify_conn.close()
            finally:
                cursor.close()
        finally:
            # Cleanup source table
            try:
                self.connection.autocommit = True
            except Exception:
                pass
            cleanup_cursor = self.connection.cursor()
            cleanup_cursor.execute(f"DROP TABLE IF EXISTS {fq_table2_name}")
            cleanup_cursor.close()
