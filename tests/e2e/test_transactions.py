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


@pytest.mark.skip(
    reason="Test environment does not yet support multi-statement transactions"
)
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
                TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')
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
            TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')
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
            TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')
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
