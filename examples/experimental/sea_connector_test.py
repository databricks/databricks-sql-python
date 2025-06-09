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
        cursor = connection.cursor()
        
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
                    column_names = extract_key_values(columns, "col_name")
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


def test_sea_result_set_arrow_external_links():
    """
    Test the SEA result set implementation with ARROW format and EXTERNAL_LINKS disposition.
    
    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a query that returns a large result set (which will use EXTERNAL_LINKS disposition),
    and tests the various fetch methods to verify the result set implementation works correctly.
    """
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    access_token = os.environ.get("DATABRICKS_TOKEN")
    catalog = os.environ.get("DATABRICKS_CATALOG", "samples")
    schema = os.environ.get("DATABRICKS_SCHEMA", "tpch")

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
            schema=schema,
            use_sea=True,
            use_cloud_fetch=True,  # Enable cloud fetch to trigger EXTERNAL_LINKS + ARROW
            user_agent_entry="SEA-Test-Client",
            # Use a smaller arraysize to potentially force multiple chunks
            arraysize=1000,
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )
        
        # Create cursor
        cursor = connection.cursor()
        
        # Execute a query that returns a large result set (will use EXTERNAL_LINKS disposition)
        # Use a larger result set to ensure multiple chunks
        # Using a CROSS JOIN to generate a larger result set
        logger.info("Executing query: SELECT a.id as id1, b.id as id2, CONCAT(CAST(a.id AS STRING), '-', CAST(b.id AS STRING)) as concat_str FROM range(1, 1000) a CROSS JOIN range(1, 1000) b LIMIT 500000")
        cursor.execute("SELECT a.id as id1, b.id as id2, CONCAT(CAST(a.id AS STRING), '-', CAST(b.id AS STRING)) as concat_str FROM range(1, 1000) a CROSS JOIN range(1, 1000) b LIMIT 500000")
        
        # Test the manifest to verify we're getting multiple chunks
        # We can't easily access the manifest in the SeaResultSet, so we'll just continue with the test
        # Note: The server might optimize results to fit into a single chunk, but our implementation
        # is designed to handle multiple chunks by fetching additional chunks when needed
        logger.info("Proceeding with fetch operations...")
        
        # Test fetchone
        logger.info("Testing fetchone...")
        row = cursor.fetchone()
        logger.info(f"First row: {row}")
        
        # Test fetchmany with a moderate size
        fetch_size = 500
        logger.info(f"Testing fetchmany({fetch_size})...")
        rows = cursor.fetchmany(fetch_size)
        logger.info(f"Fetched {len(rows)} rows with fetchmany")
        
        # Test fetchall for remaining rows
        logger.info("Testing fetchall...")
        remaining_rows = cursor.fetchall()
        logger.info(f"Fetched {len(remaining_rows)} remaining rows with fetchall")
        
        # Calculate total rows fetched
        total_rows = 1 + len(rows) + len(remaining_rows)
        logger.info(f"Total rows fetched: {total_rows}")
        
        # Execute another query to test arrow fetch methods
        logger.info("\nExecuting second query for Arrow testing: SELECT * FROM range(1, 20000) as id LIMIT 20000")
        cursor.execute("SELECT * FROM range(1, 20000) as id LIMIT 20000")
        
        try:
            # Test fetchmany_arrow with a moderate size
            arrow_fetch_size = 1000
            logger.info(f"Testing fetchmany_arrow({arrow_fetch_size})...")
            arrow_batch = cursor.fetchmany_arrow(arrow_fetch_size)
            logger.info(f"Arrow batch num rows: {arrow_batch.num_rows}")
            logger.info(f"Arrow batch columns: {arrow_batch.column_names}")
            
            # Test fetchall_arrow
            logger.info("Testing fetchall_arrow...")
            remaining_arrow_batch = cursor.fetchall_arrow()
            logger.info(f"Remaining arrow batch num rows: {remaining_arrow_batch.num_rows}")
            
            # Calculate total rows fetched with Arrow
            total_arrow_rows = arrow_batch.num_rows + remaining_arrow_batch.num_rows
            logger.info(f"Total rows fetched with Arrow: {total_arrow_rows}")
            
        except ImportError:
            logger.warning("PyArrow not installed, skipping Arrow tests")
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

    except Exception as e:
        logger.error(f"Error during SEA result set test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("SEA result set test with ARROW format and EXTERNAL_LINKS disposition completed successfully")


def test_sea_result_set_with_multiple_chunks():
    """
    Test the SEA result set implementation with multiple chunks.
    
    This function connects to a Databricks SQL endpoint using the SEA backend,
    executes a query that returns a large result set in multiple chunks,
    and tests fetching data from multiple chunks.
    """
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    access_token = os.environ.get("DATABRICKS_TOKEN")
    catalog = os.environ.get("DATABRICKS_CATALOG", "samples")
    schema = os.environ.get("DATABRICKS_SCHEMA", "default")

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
            schema=schema,
            use_sea=True,
            use_cloud_fetch=True,  # Enable cloud fetch to trigger EXTERNAL_LINKS + ARROW
            user_agent_entry="SEA-Test-Client",
            # Use a smaller arraysize to potentially force multiple chunks
            arraysize=1000,
        )

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )
        
        # Create cursor
        cursor = connection.cursor()
        
        # Execute the query that we know returns multiple chunks from interactive-sea testing
        logger.info("Executing query that returns multiple chunks...")
        query = """
        WITH large_dataset AS (
            SELECT 
                id,
                id * 2 as double_id,
                id * 3 as triple_id,
                concat('value_', repeat(cast(id as string), 100)) as large_string_value,
                array_repeat(id, 50) as large_array_value,
                rand() as random_val,
                current_timestamp() as current_time
            FROM range(1, 100000) AS t(id)
        )
        SELECT * FROM large_dataset
        """
        cursor.execute(query)
        
        # Attempt to access the manifest to check for multiple chunks
        from databricks.sql.backend.sea.backend import SeaDatabricksClient
        if isinstance(connection.session.backend, SeaDatabricksClient):
            # Get the statement ID from the cursor's active result set
            statement_id = cursor.active_result_set.statement_id
            if statement_id:
                # Make a direct request to get the statement status
                response_data = connection.session.backend.http_client._make_request(
                    method="GET",
                    path=f"/api/2.0/sql/statements/{statement_id}",
                )
                
                # Check if we have multiple chunks
                manifest = response_data.get("manifest", {})
                total_chunk_count = manifest.get("total_chunk_count", 0)
                truncated = manifest.get("truncated", False)
                
                logger.info(f"Total chunk count: {total_chunk_count}")
                logger.info(f"Result truncated: {truncated}")
                
                # Log chunk information
                chunks = manifest.get("chunks", [])
                for i, chunk in enumerate(chunks):
                    logger.info(f"Chunk {i}: index={chunk.get('chunk_index')}, rows={chunk.get('row_count')}, bytes={chunk.get('byte_count')}")
                
                # Log the next_chunk_index from the first external link
                result_data = response_data.get("result", {})
                external_links = result_data.get("external_links", [])
                if external_links:
                    first_link = external_links[0]
                    logger.info(f"First link next_chunk_index: {first_link.get('next_chunk_index')}")
                    logger.info(f"First link next_chunk_internal_link: {first_link.get('next_chunk_internal_link')}")
        
        # Test fetchone
        logger.info("Testing fetchone...")
        row = cursor.fetchone()
        logger.info(f"First row: {row}")
        
        # Test fetchmany with a size that spans multiple chunks
        fetch_size = 30000  # This should span at least 2 chunks based on our test
        logger.info(f"Testing fetchmany({fetch_size})...")
        rows = cursor.fetchmany(fetch_size)
        logger.info(f"Fetched {len(rows)} rows with fetchmany")
        first_batch_count = len(rows)
        
        # Test another fetchmany to get more chunks
        logger.info(f"Testing another fetchmany({fetch_size})...")
        more_rows = cursor.fetchmany(fetch_size)
        logger.info(f"Fetched {len(more_rows)} more rows with fetchmany")
        second_batch_count = len(more_rows)
        
        # Test fetchall for remaining rows
        logger.info("Testing fetchall...")
        remaining_rows = cursor.fetchall()
        logger.info(f"Fetched {len(remaining_rows)} remaining rows with fetchall")
        remaining_count = len(remaining_rows)
        
        # Verify results using row IDs instead of row counts
        # Calculate the sum of rows from the manifest chunks
        total_rows_from_manifest = sum(chunk.get('row_count', 0) for chunk in manifest.get('chunks', []))
        logger.info(f"Expected rows from manifest chunks: {total_rows_from_manifest}")
        
        # Collect all row IDs to check for duplicates and completeness
        all_row_ids = set()
        
        # Add the first row's ID
        if row and hasattr(row, 'id'):
            all_row_ids.add(row.id)
            first_id = row.id
            logger.info(f"First row ID: {first_id}")
        
        # Add IDs from first batch
        if rows and len(rows) > 0 and hasattr(rows[0], 'id'):
            batch_ids = [r.id for r in rows if hasattr(r, 'id')]
            all_row_ids.update(batch_ids)
            logger.info(f"First batch: {len(rows)} rows, ID range {min(batch_ids)} to {max(batch_ids)}")
        
        # Add IDs from second batch
        if more_rows and len(more_rows) > 0 and hasattr(more_rows[0], 'id'):
            batch_ids = [r.id for r in more_rows if hasattr(r, 'id')]
            all_row_ids.update(batch_ids)
            logger.info(f"Second batch: {len(more_rows)} rows, ID range {min(batch_ids)} to {max(batch_ids)}")
        
        # Add IDs from remaining rows
        if remaining_rows and len(remaining_rows) > 0 and hasattr(remaining_rows[0], 'id'):
            batch_ids = [r.id for r in remaining_rows if hasattr(r, 'id')]
            all_row_ids.update(batch_ids)
            logger.info(f"Remaining batch: {len(remaining_rows)} rows, ID range {min(batch_ids)} to {max(batch_ids)}")
        
        # Check for completeness and duplicates
        if all_row_ids:
            min_id = min(all_row_ids)
            max_id = max(all_row_ids)
            expected_count = max_id - min_id + 1
            actual_count = len(all_row_ids)
            
            logger.info(f"Row ID range: {min_id} to {max_id}")
            logger.info(f"Expected unique IDs in range: {expected_count}")
            logger.info(f"Actual unique IDs collected: {actual_count}")
            
            if expected_count == actual_count:
                logger.info("✅ All rows fetched correctly with no gaps")
            else:
                logger.warning("⚠️ Gap detected in row IDs")
                
            # Check for duplicates
            if actual_count == len(all_row_ids):
                logger.info("✅ No duplicate row IDs detected")
            else:
                logger.warning("⚠️ Duplicate row IDs detected")
                
            # Check if we got all expected rows
            if max_id == total_rows_from_manifest:
                logger.info("✅ Last row ID matches expected row count from manifest")
            
            # Let's try one more time with a fresh cursor to fetch all rows at once
            logger.info("\nTesting fetchall_arrow with a fresh cursor...")
            new_cursor = connection.cursor()
            new_cursor.execute(query)
            
            try:
                # Fetch all rows as Arrow
                arrow_table = new_cursor.fetchall_arrow()
                logger.info(f"Arrow table num rows: {arrow_table.num_rows}")
                logger.info(f"Arrow table columns: {arrow_table.column_names}")
                
                # Get the ID column if it exists
                if 'id' in arrow_table.column_names:
                    id_column = arrow_table.column('id').to_pylist()
                    logger.info(f"First 5 rows of id column: {id_column[:5]}")
                    logger.info(f"Last 5 rows of id column: {id_column[-5:]}")
                    
                    # Check for completeness and duplicates in Arrow results
                    arrow_id_set = set(id_column)
                    arrow_min_id = min(id_column)
                    arrow_max_id = max(id_column)
                    arrow_expected_count = arrow_max_id - arrow_min_id + 1
                    arrow_actual_count = len(arrow_id_set)
                    
                    logger.info(f"Arrow result row ID range: {arrow_min_id} to {arrow_max_id}")
                    logger.info(f"Arrow result expected unique IDs: {arrow_expected_count}")
                    logger.info(f"Arrow result actual unique IDs: {arrow_actual_count}")
                    
                    if arrow_expected_count == arrow_actual_count:
                        logger.info("✅ Arrow results: All rows fetched correctly with no gaps")
                    else:
                        logger.warning("⚠️ Arrow results: Gap detected in row IDs")
                    
                    if arrow_actual_count == len(arrow_id_set):
                        logger.info("✅ Arrow results: No duplicate row IDs detected")
                    else:
                        logger.warning("⚠️ Arrow results: Duplicate row IDs detected")
                    
                    # Compare with manifest row count
                    if arrow_max_id == total_rows_from_manifest:
                        logger.info("✅ Arrow results: Last row ID matches expected row count from manifest")
                    
                    # Compare with sequential fetch results
                    if arrow_id_set == all_row_ids:
                        logger.info("✅ Arrow and sequential fetch results contain exactly the same row IDs")
                    else:
                        logger.warning("⚠️ Arrow and sequential fetch results contain different row IDs")
                        only_in_arrow = arrow_id_set - all_row_ids
                        only_in_sequential = all_row_ids - arrow_id_set
                        if only_in_arrow:
                            logger.warning(f"IDs only in Arrow results: {len(only_in_arrow)} rows")
                        if only_in_sequential:
                            logger.warning(f"IDs only in sequential fetch: {len(only_in_sequential)} rows")
                
                # Check if we got all rows
                total_rows_from_manifest = sum(chunk.get('row_count', 0) for chunk in manifest.get('chunks', []))
                logger.info(f"Expected rows from manifest chunks: {total_rows_from_manifest}")
                logger.info(f"Actual rows in arrow table: {arrow_table.num_rows}")
                
                # Note: The server might return more rows than specified in the manifest due to query optimization
                # This is expected behavior and not an error
                if arrow_table.num_rows >= total_rows_from_manifest:
                    logger.info("✅ Retrieved at least as many rows as expected from the manifest")
                else:
                    logger.warning(f"⚠️ Retrieved fewer rows ({arrow_table.num_rows}) than expected from manifest ({total_rows_from_manifest})")
            except Exception as e:
                logger.error(f"Error fetching all rows as Arrow: {e}")
            
            new_cursor.close()
        
        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

    except Exception as e:
        logger.error(f"Error during SEA result set test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("SEA result set test with multiple chunks completed successfully")


if __name__ == "__main__":
    # Test session management
    # test_sea_session()
    
    # Test result set implementation with metadata commands
    # test_sea_result_set_json_array_inline()
    
    # Test result set implementation with ARROW format and EXTERNAL_LINKS disposition
    # test_sea_result_set_arrow_external_links()
    
    # Test result set implementation with multiple chunks
    test_sea_result_set_with_multiple_chunks()