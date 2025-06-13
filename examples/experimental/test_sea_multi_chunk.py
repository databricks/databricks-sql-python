"""
Test for SEA multi-chunk responses.

This script tests the SEA connector's ability to handle multi-chunk responses correctly.
It runs queries that generate large rows to force multiple chunks and verifies that
the correct number of rows are returned.
"""
import os
import sys
import logging
import time
from databricks.sql.client import Connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sea_multi_chunk_with_cloud_fetch(requested_row_count=5000):
    """
    Test executing a query that generates multiple chunks using cloud fetch.

    Args:
        requested_row_count: Number of rows to request in the query

    Returns:
        bool: True if the test passed, False otherwise
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
        logger.info("Creating connection for query execution with cloud fetch enabled")
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
        cursor = connection.cursor()
        query = f"""
        SELECT 
            id, 
            concat('value_', repeat('a', 10000)) as test_value
        FROM range(1, {requested_row_count} + 1) AS t(id)
        """

        logger.info(
            f"Executing query with cloud fetch to generate {requested_row_count} rows"
        )
        start_time = time.time()
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()
        actual_row_count = len(rows)
        end_time = time.time()

        logger.info(f"Query executed in {end_time - start_time:.2f} seconds")
        logger.info(
            f"Requested {requested_row_count} rows, received {actual_row_count} rows"
        )

        # Verify row count
        success = actual_row_count == requested_row_count
        if success:
            logger.info("✅ PASSED: Received correct number of rows")
        else:
            logger.error(
                f"❌ FAILED: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}"
            )

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return success

    except Exception as e:
        logger.error(f"Error during SEA multi-chunk test with cloud fetch: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return False


def test_sea_multi_chunk_without_cloud_fetch(requested_row_count=100):
    """
    Test executing a query that generates multiple chunks without using cloud fetch.

    Args:
        requested_row_count: Number of rows to request in the query

    Returns:
        bool: True if the test passed, False otherwise
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
        logger.info("Creating connection for query execution with cloud fetch disabled")
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
        cursor = connection.cursor()
        query = f"""
        SELECT 
            id, 
            concat('value_', repeat('a', 100)) as test_value
        FROM range(1, {requested_row_count} + 1) AS t(id)
        """

        logger.info(
            f"Executing query without cloud fetch to generate {requested_row_count} rows"
        )
        start_time = time.time()
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()
        actual_row_count = len(rows)
        end_time = time.time()

        logger.info(f"Query executed in {end_time - start_time:.2f} seconds")
        logger.info(
            f"Requested {requested_row_count} rows, received {actual_row_count} rows"
        )

        # Verify row count
        success = actual_row_count == requested_row_count
        if success:
            logger.info("✅ PASSED: Received correct number of rows")
        else:
            logger.error(
                f"❌ FAILED: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}"
            )

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return success

    except Exception as e:
        logger.error(f"Error during SEA multi-chunk test without cloud fetch: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return False


def main():
    # Check if required environment variables are set
    required_vars = [
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_TOKEN",
    ]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        logger.error(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
        logger.error("Please set these variables before running the tests.")
        sys.exit(1)

    # Get row count from command line or use default
    cloud_fetch_row_count = 5000
    non_cloud_fetch_row_count = 100

    if len(sys.argv) > 1:
        try:
            cloud_fetch_row_count = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid row count for cloud fetch: {sys.argv[1]}")
            logger.error("Please provide a valid integer for row count.")
            sys.exit(1)

    if len(sys.argv) > 2:
        try:
            non_cloud_fetch_row_count = int(sys.argv[2])
        except ValueError:
            logger.error(f"Invalid row count for non-cloud fetch: {sys.argv[2]}")
            logger.error("Please provide a valid integer for row count.")
            sys.exit(1)

    logger.info(
        f"Testing with {cloud_fetch_row_count} rows for cloud fetch and {non_cloud_fetch_row_count} rows for non-cloud fetch"
    )

    # Test with cloud fetch
    with_cloud_fetch_success = test_sea_multi_chunk_with_cloud_fetch(
        cloud_fetch_row_count
    )
    logger.info(
        f"Multi-chunk test with cloud fetch: {'✅ PASSED' if with_cloud_fetch_success else '❌ FAILED'}"
    )

    # Test without cloud fetch
    without_cloud_fetch_success = test_sea_multi_chunk_without_cloud_fetch(
        non_cloud_fetch_row_count
    )
    logger.info(
        f"Multi-chunk test without cloud fetch: {'✅ PASSED' if without_cloud_fetch_success else '❌ FAILED'}"
    )

    # Compare results
    logger.info("\n=== RESULTS SUMMARY ===")
    logger.info(
        f"Cloud fetch test ({cloud_fetch_row_count} rows): {'✅ PASSED' if with_cloud_fetch_success else '❌ FAILED'}"
    )
    logger.info(
        f"Non-cloud fetch test ({non_cloud_fetch_row_count} rows): {'✅ PASSED' if without_cloud_fetch_success else '❌ FAILED'}"
    )

    if with_cloud_fetch_success and without_cloud_fetch_success:
        logger.info("✅ ALL TESTS PASSED")
        sys.exit(0)
    else:
        logger.info("❌ SOME TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
