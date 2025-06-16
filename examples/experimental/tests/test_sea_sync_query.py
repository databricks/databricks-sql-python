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
    executes a query with cloud fetch enabled, and verifies that execution completes successfully.
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

        # Execute a query that generates large rows to force multiple chunks
        requested_row_count = 10000
        cursor = connection.cursor()
        query = f"""
        SELECT 
            id, 
            concat('value_', repeat('a', 10000)) as test_value
        FROM range(1, {requested_row_count} + 1) AS t(id)
        """

        logger.info(
            f"Executing synchronous query with cloud fetch to generate {requested_row_count} rows"
        )
        cursor.execute(query)

        # Use a mix of fetch methods to retrieve all rows
        logger.info("Retrieving data using a mix of fetch methods")
        
        # First, get one row with fetchone
        first_row = cursor.fetchone()
        if not first_row:
            logger.error("FAIL: fetchone returned None, expected a row")
            return False
        
        logger.info(f"Successfully retrieved first row with ID: {first_row[0]}")
        retrieved_rows = [first_row]
        
        # Then, get a batch of rows with fetchmany
        batch_size = 100
        batch_rows = cursor.fetchmany(batch_size)
        logger.info(f"Successfully retrieved {len(batch_rows)} rows with fetchmany")
        retrieved_rows.extend(batch_rows)
        
        # Finally, get all remaining rows with fetchall
        remaining_rows = cursor.fetchall()
        logger.info(f"Successfully retrieved {len(remaining_rows)} rows with fetchall")
        retrieved_rows.extend(remaining_rows)
        
        # Calculate total row count
        actual_row_count = len(retrieved_rows)
        
        logger.info(
            f"Requested {requested_row_count} rows, received {actual_row_count} rows"
        )
        
        # Verify total row count
        if actual_row_count != requested_row_count:
            logger.error(
                f"FAIL: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}"
            )
            return False
        
        logger.info("PASS: Received correct number of rows with cloud fetch and all fetch methods work correctly")

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
    executes a query with cloud fetch disabled, and verifies that execution completes successfully.
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
            f"Executing synchronous query without cloud fetch to generate {requested_row_count} rows"
        )
        cursor.execute(query)

        # Use a mix of fetch methods to retrieve all rows
        logger.info("Retrieving data using a mix of fetch methods")
        
        # First, get one row with fetchone
        first_row = cursor.fetchone()
        if not first_row:
            logger.error("FAIL: fetchone returned None, expected a row")
            return False
        
        logger.info(f"Successfully retrieved first row with ID: {first_row[0]}")
        retrieved_rows = [first_row]
        
        # Then, get a batch of rows with fetchmany
        batch_size = 10  # Smaller batch size for non-cloud fetch
        batch_rows = cursor.fetchmany(batch_size)
        logger.info(f"Successfully retrieved {len(batch_rows)} rows with fetchmany")
        retrieved_rows.extend(batch_rows)
        
        # Finally, get all remaining rows with fetchall
        remaining_rows = cursor.fetchall()
        logger.info(f"Successfully retrieved {len(remaining_rows)} rows with fetchall")
        retrieved_rows.extend(remaining_rows)
        
        # Calculate total row count
        actual_row_count = len(retrieved_rows)
        
        logger.info(
            f"Requested {requested_row_count} rows, received {actual_row_count} rows"
        )
        
        # Verify total row count
        if actual_row_count != requested_row_count:
            logger.error(
                f"FAIL: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}"
            )
            return False
        
        logger.info("PASS: Received correct number of rows without cloud fetch and all fetch methods work correctly")

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
