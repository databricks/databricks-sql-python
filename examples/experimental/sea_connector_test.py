import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_sea_result_set_json_array_inline():
    """
    Test the SEA result set implementation with JSON_ARRAY format and INLINE disposition.
    
    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a query that returns a small result set (which will use INLINE disposition),
    and tests the various fetch methods to verify the result set implementation works correctly.
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
        logger.info("Creating connection with SEA backend...")
        connection = Connection(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="default",
            use_sea=True,
            use_cloud_fetch=False, # trigger INLINE + JSON_ARRAY 
            user_agent_entry="SEA-Test-Client",
            enable_query_result_lz4_compression=False,
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )
        
        # Create cursor
        cursor = connection.cursor(arraysize=0, buffer_size_bytes=0)
        
        # Execute a query that returns a small result set (will use INLINE disposition)
        logger.info("Executing query: SELECT * FROM range(1, 10) AS id")
        cursor.execute("SELECT * FROM range(1, 10) AS id")
        
        # Test fetchone
        logger.info("Testing fetchone...")
        row = cursor.fetchone()
        logger.info(f"First row: {row}")
        
        # Test fetchmany
        logger.info("Testing fetchmany(3)...")
        rows = cursor.fetchmany(3)
        logger.info(f"Next 3 rows: {rows}")
        
        # Test fetchall
        logger.info("Testing fetchall...")
        remaining_rows = cursor.fetchall()
        logger.info(f"Remaining rows: {remaining_rows}")
        
        # Execute another query to test arrow fetch methods
        logger.info("Executing query for Arrow testing: SELECT * FROM range(1, 5) AS id, range(101, 105) AS value")
        cursor.execute("SELECT * FROM range(1, 5) AS id, range(101, 105) AS value")
        
        # Test fetchmany_arrow
        logger.info("Testing fetchmany_arrow(2)...")
        arrow_batch = cursor.fetchmany_arrow(2)
        logger.info(f"Arrow batch num rows: {arrow_batch.num_rows}")
        logger.info(f"Arrow batch columns: {arrow_batch.column_names}")
        logger.info(f"Arrow batch data: {arrow_batch.to_pydict()}")
        
        # Test fetchall_arrow
        logger.info("Testing fetchall_arrow...")
        remaining_arrow_batch = cursor.fetchall_arrow()
        logger.info(f"Remaining arrow batch num rows: {remaining_arrow_batch.num_rows}")
        logger.info(f"Remaining arrow batch data: {remaining_arrow_batch.to_pydict()}")
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

    except Exception as e:
        logger.error(f"Error during SEA result set test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("SEA result set test with JSON_ARRAY format and INLINE disposition completed successfully")


if __name__ == "__main__":
    # Test result set implementation
    test_sea_result_set_json_array_inline()