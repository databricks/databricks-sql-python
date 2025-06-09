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
        logger.info("Creating connection for asynchronous query execution with cloud fetch enabled")
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

        # Execute a simple query asynchronously
        cursor = connection.cursor()
        logger.info("Executing asynchronous query with cloud fetch: SELECT 1 as test_value")
        cursor.execute_async("SELECT 1 as test_value")
        logger.info("Asynchronous query submitted successfully with cloud fetch enabled")
        
        # Check query state
        logger.info("Checking query state...")
        while cursor.is_query_pending():
            logger.info("Query is still pending, waiting...")
            time.sleep(1)
            
        logger.info("Query is no longer pending, getting results...")
        cursor.get_async_execution_result()
        logger.info("Successfully retrieved asynchronous query results with cloud fetch enabled")
        
        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")
        
        return True

    except Exception as e:
        logger.error(f"Error during SEA asynchronous query execution test with cloud fetch: {str(e)}")
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
        logger.info("Creating connection for asynchronous query execution with cloud fetch disabled")
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

        # Execute a simple query asynchronously
        cursor = connection.cursor()
        logger.info("Executing asynchronous query without cloud fetch: SELECT 1 as test_value")
        cursor.execute_async("SELECT 1 as test_value")
        logger.info("Asynchronous query submitted successfully with cloud fetch disabled")
        
        # Check query state
        logger.info("Checking query state...")
        while cursor.is_query_pending():
            logger.info("Query is still pending, waiting...")
            time.sleep(1)
            
        logger.info("Query is no longer pending, getting results...")
        cursor.get_async_execution_result()
        logger.info("Successfully retrieved asynchronous query results with cloud fetch disabled")
        
        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")
        
        return True

    except Exception as e:
        logger.error(f"Error during SEA asynchronous query execution test without cloud fetch: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_sea_async_query_exec():
    """
    Run both asynchronous query tests and return overall success.
    """
    with_cloud_fetch_success = test_sea_async_query_with_cloud_fetch()
    logger.info(f"Asynchronous query with cloud fetch: {'✅ PASSED' if with_cloud_fetch_success else '❌ FAILED'}")
    
    without_cloud_fetch_success = test_sea_async_query_without_cloud_fetch()
    logger.info(f"Asynchronous query without cloud fetch: {'✅ PASSED' if without_cloud_fetch_success else '❌ FAILED'}")
    
    return with_cloud_fetch_success and without_cloud_fetch_success


if __name__ == "__main__":
    success = test_sea_async_query_exec()
    sys.exit(0 if success else 1)