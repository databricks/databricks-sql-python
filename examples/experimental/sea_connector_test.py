import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_sea_query_execution():
    """
    Test executing a query using the SEA backend.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a simple query, and verifies that execution completes successfully.
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
        sys.exit(1)

    try:
        # Create connection with SEA backend
        connection = Connection(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="default",
            use_sea=True,
            user_agent_entry="SEA-Test-Client",
            use_cloud_fetch=False,
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )
        logger.info(f"backend type: {type(connection.session.backend)}")

        # Create a cursor and execute a simple query
        cursor = connection.cursor(arraysize=0, buffer_size_bytes=0)

        logger.info("Executing query: SELECT 1 as test_value")
        cursor.execute("SELECT 1 as test_value")

        # We don't fetch results yet since we haven't implemented the fetch functionality
        logger.info("Query executed successfully")

        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

    except Exception as e:
        logger.error(f"Error during SEA query execution test: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("SEA query execution test completed successfully")


def test_sea_session():
    """
    Test opening and closing a SEA session using the connector.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    opens a session, and then closes it.

    Required environment variables:
    - DATABRICKS_SERVER_HOSTNAME: Databricks server hostname
    - DATABRICKS_HTTP_PATH: HTTP path for the SQL endpoint
    - DATABRICKS_TOKEN: Personal access token for authentication
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
        sys.exit(1)

    logger.info(f"Connecting to {server_hostname}")
    logger.info(f"HTTP Path: {http_path}")
    if catalog:
        logger.info(f"Using catalog: {catalog}")

    try:
        logger.info("Creating connection with SEA backend...")
        connection = Connection(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="default",
            use_sea=True,
            user_agent_entry="SEA-Test-Client",  # add custom user agent
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )
        logger.info(f"backend type: {type(connection.session.backend)}")

        # Close the connection
        logger.info("Closing the SEA session...")
        connection.close()
        logger.info("Successfully closed SEA session")

    except Exception as e:
        logger.error(f"Error testing SEA session: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("SEA session test completed successfully")


if __name__ == "__main__":
    # Test session management
    test_sea_session()

    # Test query execution
    test_sea_query_execution()
