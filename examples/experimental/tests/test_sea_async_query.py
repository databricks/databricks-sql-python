"""
Test for SEA asynchronous query execution functionality.
"""
import os
import sys
import logging
import time
from databricks.sql.client import Connection
from databricks.sql.backend.types import CommandState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sea_async_query_with_cloud_fetch():
    """
    Test executing a query asynchronously using the SEA backend with cloud fetch enabled.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a simple query asynchronously with cloud fetch enabled, and verifies that execution completes successfully.
    """
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    access_token = os.environ.get("DATABRICKS_TOKEN")
    catalog = os.environ.get("DATABRICKS_CATALOG")

    if not all([server_hostname, http_path, access_token]):
        logger.error("Missing required environment variables.")
        logger.error(
            "Please set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN."
        )
        return False

    try:
        # Create connection with cloud fetch enabled
        logger.info(
            "Creating connection for asynchronous query execution with cloud fetch enabled"
        )
        connection = Connection(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="default",
            use_sea=True,
            user_agent_entry="SEA-Test-Client",
            use_cloud_fetch=True,
            enable_query_result_lz4_compression=False,
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )

        # Execute a query that generates large rows to force multiple chunks
        requested_row_count = 5000
        cursor = connection.cursor()
        query = f"""
        SELECT 
            id, 
            concat('value_', repeat('a', 10000)) as test_value
        FROM range(1, {requested_row_count} + 1) AS t(id)
        """

        logger.info(
            f"Executing asynchronous query with cloud fetch to generate {requested_row_count} rows"
        )
        cursor.execute_async(query)
        logger.info(
            "Asynchronous query submitted successfully with cloud fetch enabled"
        )

        # Check query state
        logger.info("Checking query state...")
        while cursor.is_query_pending():
            logger.info("Query is still pending, waiting...")
            time.sleep(1)

        logger.info("Query is no longer pending, getting results...")
        cursor.get_async_execution_result()

        results = [cursor.fetchone()]
        results.extend(cursor.fetchmany(10))
        results.extend(cursor.fetchall())
        actual_row_count = len(results)

        logger.info(
            f"Requested {requested_row_count} rows, received {actual_row_count} rows"
        )

        # Verify total row count
        if actual_row_count != requested_row_count:
            logger.error(
                f"FAIL: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}"
            )
            return False

        logger.info(
            "PASS: Received correct number of rows with cloud fetch and all fetch methods work correctly"
        )

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return True

    except Exception as e:
        logger.error(
            f"Error during SEA asynchronous query execution test with cloud fetch: {str(e)}"
        )
        import traceback

        logger.error(traceback.format_exc())
        return False


def test_sea_async_query_without_cloud_fetch():
    """
    Test executing a query asynchronously using the SEA backend with cloud fetch disabled.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a simple query asynchronously with cloud fetch disabled, and verifies that execution completes successfully.
    """
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    access_token = os.environ.get("DATABRICKS_TOKEN")
    catalog = os.environ.get("DATABRICKS_CATALOG")

    if not all([server_hostname, http_path, access_token]):
        logger.error("Missing required environment variables.")
        logger.error(
            "Please set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN."
        )
        return False

    try:
        # Create connection with cloud fetch disabled
        logger.info(
            "Creating connection for asynchronous query execution with cloud fetch disabled"
        )
        connection = Connection(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="default",
            use_sea=True,
            user_agent_entry="SEA-Test-Client",
            use_cloud_fetch=False,
            enable_query_result_lz4_compression=False,
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )

        # For non-cloud fetch, use a smaller row count to avoid exceeding inline limits
        requested_row_count = 100
        cursor = connection.cursor()
        query = f"""
        SELECT 
            id, 
            concat('value_', repeat('a', 100)) as test_value
        FROM range(1, {requested_row_count} + 1) AS t(id)
        """

        logger.info(
            f"Executing asynchronous query without cloud fetch to generate {requested_row_count} rows"
        )
        cursor.execute_async(query)
        logger.info(
            "Asynchronous query submitted successfully with cloud fetch disabled"
        )

        # Check query state
        logger.info("Checking query state...")
        while cursor.is_query_pending():
            logger.info("Query is still pending, waiting...")
            time.sleep(1)

        logger.info("Query is no longer pending, getting results...")
        cursor.get_async_execution_result()
        results = [cursor.fetchone()]
        results.extend(cursor.fetchmany(10))
        results.extend(cursor.fetchall())
        actual_row_count = len(results)

        logger.info(
            f"Requested {requested_row_count} rows, received {actual_row_count} rows"
        )

        # Verify total row count
        if actual_row_count != requested_row_count:
            logger.error(
                f"FAIL: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}"
            )
            return False

        logger.info(
            "PASS: Received correct number of rows without cloud fetch and all fetch methods work correctly"
        )

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return True

    except Exception as e:
        logger.error(
            f"Error during SEA asynchronous query execution test without cloud fetch: {str(e)}"
        )
        import traceback

        logger.error(traceback.format_exc())
        return False


def test_sea_async_query_exec():
    """
    Run both asynchronous query tests and return overall success.
    """
    with_cloud_fetch_success = test_sea_async_query_with_cloud_fetch()
    logger.info(
        f"Asynchronous query with cloud fetch: {'✅ PASSED' if with_cloud_fetch_success else '❌ FAILED'}"
    )

    without_cloud_fetch_success = test_sea_async_query_without_cloud_fetch()
    logger.info(
        f"Asynchronous query without cloud fetch: {'✅ PASSED' if without_cloud_fetch_success else '❌ FAILED'}"
    )

    return with_cloud_fetch_success and without_cloud_fetch_success


if __name__ == "__main__":
    success = test_sea_async_query_exec()
    sys.exit(0 if success else 1)
