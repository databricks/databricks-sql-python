import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def log_metadata_results(result_type: str, results, logger):
    """
    General function to log metadata results using Row.asDict() for any structure.
    
    Args:
        result_type: String describing what type of metadata (e.g., "catalogs", "schemas")
        results: List of Row objects from cursor.fetchall()
        logger: Logger instance to use for output
    """
    if not results:
        logger.info(f"No {result_type} found")
        return
        
    logger.info(f"Found {len(results)} {result_type}:")
    
    # Log all results with full details
    for i, row in enumerate(results):
        row_dict = row.asDict()
        logger.info(f"  {result_type}[{i}]: {row_dict}")
        
    # Show available fields for this result type
    if results:
        first_row_dict = results[0].asDict()
        available_fields = list(first_row_dict.keys())
        logger.info(f"  Available fields for {result_type}: {available_fields}")


def extract_key_values(results, key_field):
    """
    Extract values for a specific key field from results using Row.asDict().
    
    Args:
        results: List of Row objects
        key_field: String name of the field to extract
        
    Returns:
        List of values for the specified field, or empty list if field doesn't exist
    """
    if not results:
        return []
        
    values = []
    for row in results:
        row_dict = row.asDict()
        if key_field in row_dict:
            values.append(row_dict[key_field])
        else:
            # Log available fields if the requested key doesn't exist
            available_fields = list(row_dict.keys())
            logger.warning(f"Field '{key_field}' not found. Available fields: {available_fields}")
            break
    
    return values


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

    catalog = "samples"
    schema = "tpch"

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
        
        try:
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
        except ImportError:
            logger.warning("PyArrow not installed, skipping Arrow tests")
        
        # Test metadata commands with general logging
        logger.info("Testing metadata commands...")
        
        # Get catalogs
        logger.info("Getting catalogs...")
        cursor.catalogs()
        catalogs = cursor.fetchall()
        log_metadata_results("catalogs", catalogs, logger)
        
        # Extract catalog names using the general function
        catalog_names = extract_key_values(catalogs, "catalog")
        if catalog_names:
            logger.info(f"Catalog names: {catalog_names}")
        
        # Get schemas
        if catalog:
            logger.info(f"Getting schemas for catalog '{catalog}'...")
            cursor.schemas(catalog_name=catalog)
            schemas = cursor.fetchall()
            log_metadata_results("schemas", schemas, logger)
            
            # Extract schema names - try common field names
            schema_names = extract_key_values(schemas, "databaseName") or extract_key_values(schemas, "schemaName") or extract_key_values(schemas, "schema_name")
            if schema_names:
                logger.info(f"Schema names: {schema_names}")
            
            # Get tables for a schema
            if schemas and schema_names:
                schema = schema_names[0]
                logger.info(f"Getting tables for schema '{schema}'...")
                cursor.tables(catalog_name=catalog, schema_name=schema)
                tables = cursor.fetchall()
                log_metadata_results("tables", tables, logger)
                
                # Extract table names
                table_names = extract_key_values(tables, "tableName") or extract_key_values(tables, "table_name")
                if table_names:
                    logger.info(f"Table names: {table_names}")
                
                # Get columns for a table
                if tables and table_names:
                    table = table_names[0]
                    logger.info(f"Getting columns for table '{table}'...")
                    cursor.columns(catalog_name=catalog, schema_name=schema, table_name=table)
                    columns = cursor.fetchall()
                    log_metadata_results("columns", columns, logger)
                    
                    # Extract column names
                    column_names = extract_key_values(columns, "column_name") or extract_key_values(columns, "columnName") or extract_key_values(columns, "COLUMN_NAME")
                    if column_names:
                        logger.info(f"Column names: {column_names}")
        
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
    
    # Test result set implementation with metadata commands
    test_sea_result_set_json_array_inline()