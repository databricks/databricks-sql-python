"""
Test for SEA metadata functionality.
"""
import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sea_metadata():
    """
    Test metadata operations using the SEA backend.

    This function connects to a Databricks SQL endpoint using the SEA backend,
    and executes metadata operations like catalogs(), schemas(), tables(), and columns().
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

    if not catalog:
        logger.error(
            "DATABRICKS_CATALOG environment variable is required for metadata tests."
        )
        return False

    try:
        # Create connection
        logger.info("Creating connection for metadata operations")
        connection = Connection(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="default",
            use_sea=True,
            user_agent_entry="SEA-Test-Client",
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )

        # Test catalogs
        cursor = connection.cursor()
        logger.info("Fetching catalogs...")
        cursor.catalogs()
        rows = cursor.fetchall()
        logger.info(f"Rows: {rows}")
        logger.info("Successfully fetched catalogs")

        # Test schemas
        logger.info(f"Fetching schemas for catalog '{catalog}'...")
        cursor.schemas(catalog_name=catalog)
        rows = cursor.fetchall()
        logger.info(f"Rows: {rows}")
        logger.info("Successfully fetched schemas")

        # Test tables
        logger.info(f"Fetching tables for catalog '{catalog}', schema 'default'...")
        cursor.tables(catalog_name=catalog, schema_name="default")
        rows = cursor.fetchall()
        logger.info(f"Rows: {rows}")
        logger.info("Successfully fetched tables")

        # Test columns for a specific table
        # Using a common table that should exist in most environments
        logger.info(
            f"Fetching columns for catalog '{catalog}', schema 'default', table 'customer'..."
        )
        cursor.columns(
            catalog_name=catalog, schema_name="default", table_name="customer"
        )
        rows = cursor.fetchall()
        logger.info(f"Rows: {rows}")
        logger.info("Successfully fetched columns")

        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

        return True

    except Exception as e:
        logger.error(f"Error during SEA metadata test: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = test_sea_metadata()
    sys.exit(0 if success else 1)
