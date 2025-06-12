"""
Test for SEA synchronous query execution functionality.
"""
import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sea_sync_query_with_cloud_fetch():
    """
    Test executing a query synchronously using the SEA backend with cloud fetch enabled.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a simple query with cloud fetch enabled, and verifies that execution completes successfully.
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
            "Creating connection for synchronous query execution with cloud fetch enabled"
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
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )

        # Execute a simple query
        cursor = connection.cursor()
        logger.info(
            "Executing synchronous query with cloud fetch: SELECT 1 as test_value"
        )
        cursor.execute("SELECT 1 as test_value")
        logger.info("Query executed successfully with cloud fetch enabled")

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return True

    except Exception as e:
        logger.error(
            f"Error during SEA synchronous query execution test with cloud fetch: {str(e)}"
        )
        import traceback

        logger.error(traceback.format_exc())
        return False


def test_sea_sync_query_without_cloud_fetch():
    """
    Test executing a query synchronously using the SEA backend with cloud fetch disabled.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a simple query with cloud fetch disabled, and verifies that execution completes successfully.
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
            "Creating connection for synchronous query execution with cloud fetch disabled"
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

        # Execute a simple query
        cursor = connection.cursor()
        logger.info(
            "Executing synchronous query without cloud fetch: SELECT 1 as test_value"
        )
        cursor.execute("SELECT 1 as test_value")
        logger.info("Query executed successfully with cloud fetch disabled")

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return True

    except Exception as e:
        logger.error(
            f"Error during SEA synchronous query execution test without cloud fetch: {str(e)}"
        )
        import traceback

        logger.error(traceback.format_exc())
        return False


def test_sea_sync_query_exec():
    """
    Run both synchronous query tests and return overall success.
    """
    with_cloud_fetch_success = test_sea_sync_query_with_cloud_fetch()
    logger.info(
        f"Synchronous query with cloud fetch: {'✅ PASSED' if with_cloud_fetch_success else '❌ FAILED'}"
    )

    without_cloud_fetch_success = test_sea_sync_query_without_cloud_fetch()
    logger.info(
        f"Synchronous query without cloud fetch: {'✅ PASSED' if without_cloud_fetch_success else '❌ FAILED'}"
    )

    return with_cloud_fetch_success and without_cloud_fetch_success


if __name__ == "__main__":
    success = test_sea_sync_query_exec()
    sys.exit(0 if success else 1)
