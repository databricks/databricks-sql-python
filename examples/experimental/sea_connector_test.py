import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.INFO)
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


def test_multi_chunk_result_set():
    """
    Test the SEA result set implementation with a query that generates multiple chunks.
    
    This function tests fetching data from a large result set that spans multiple chunks,
    which is where the bug with incomplete results was occurring.
    """
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    access_token = os.environ.get("DATABRICKS_TOKEN")
    catalog = os.environ.get("DATABRICKS_CATALOG", "peco")
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
            use_cloud_fetch=True,  # Enable cloud fetch to use ARROW format with EXTERNAL_LINKS
            user_agent_entry="SEA-Test-Client",
            # Use a smaller arraysize to force multiple chunks
            arraysize=10000,
        )

        # Enable more detailed logging for the cloudfetch module
        logging.getLogger('databricks.sql.cloudfetch').setLevel(logging.DEBUG)
        logging.getLogger('databricks.sql.backend.sea').setLevel(logging.DEBUG)

        logger.info(
            f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}"
        )
        
        # Create cursor
        cursor = connection.cursor()
        
        # Execute a query with exactly 15,000 rows that will generate ~6 chunks
        # Using a fixed, predictable number of rows
        requested_row_count = 15000  # Request 15,000 rows
        query = f"""
        SELECT 
            id,
            -- Create a moderately large string value to ensure multiple chunks
            concat('value_', repeat('abcdefghijklmnopqrstuvwxyz0123456789', 200)) as large_string_value
        FROM range(1, {requested_row_count + 1}) AS t(id)  -- range is exclusive on upper bound, so add 1
        """
        
        logger.info(f"Executing multi-chunk query requesting {requested_row_count} rows...")
        cursor.execute(query)
        
        # Get information about chunks from the statement
        from databricks.sql.backend.sea.backend import SeaDatabricksClient
        if isinstance(connection.session.backend, SeaDatabricksClient):
            statement_id = cursor.active_result_set.statement_id
            if statement_id:
                response_data = connection.session.backend.http_client._make_request(
                    method="GET",
                    path=f"/api/2.0/sql/statements/{statement_id}",
                )
                
                manifest = response_data.get("manifest", {})
                total_chunk_count = manifest.get("total_chunk_count", 0)
                chunks = manifest.get("chunks", [])
                
                logger.info(f"Total chunk count: {total_chunk_count}")
                logger.info(f"Result truncated: {manifest.get('truncated', False)}")
                
                # Log chunk information
                for i, chunk in enumerate(chunks):
                    logger.info(f"Chunk {i}: index={chunk.get('chunk_index')}, rows={chunk.get('row_count')}, bytes={chunk.get('byte_count')}")
        
        # First, get a single row
        first_row = cursor.fetchone()
        logger.info(f"First row ID: {first_row.id if first_row else None}")
        
        # Initialize tracking variables for detailed chunk analysis
        chunk_row_counts = {}  # To track actual rows retrieved per chunk
        chunk_id_ranges = {}   # To track ID ranges per chunk
        chunk_row_ids = {}     # To track the actual IDs in each chunk
        all_ids = set()
        if first_row and hasattr(first_row, 'id'):
            all_ids.add(first_row.id)
            # Track which chunk this row came from
            chunk_index = 0  # First row always comes from chunk 0
            chunk_row_counts[chunk_index] = chunk_row_counts.get(chunk_index, 0) + 1
            chunk_id_ranges[chunk_index] = (first_row.id, first_row.id)  # (min, max)
            chunk_row_ids[chunk_index] = [first_row.id]  # Start tracking actual IDs
        
        # Fetch rows in batches to cross chunk boundaries
        batch_size = 25000
        
        # Create a function to track which chunk each row comes from
        def track_row_chunk(row_offset):
            """Determine which chunk a row belongs to based on its offset"""
            current_offset = 0
            for i, chunk in enumerate(chunks):
                chunk_size = chunk.get('row_count', 0)
                if row_offset < current_offset + chunk_size:
                    return i
                current_offset += chunk_size
            return len(chunks) - 1  # Default to last chunk if not found
        
        # Helper function to update chunk tracking data
        def update_chunk_tracking(row_id, row_offset):
            chunk_idx = track_row_chunk(row_offset)
            # Update count
            chunk_row_counts[chunk_idx] = chunk_row_counts.get(chunk_idx, 0) + 1
            # Update range
            if chunk_idx in chunk_id_ranges:
                min_id, max_id = chunk_id_ranges[chunk_idx]
                chunk_id_ranges[chunk_idx] = (min(min_id, row_id), max(max_id, row_id))
            else:
                chunk_id_ranges[chunk_idx] = (row_id, row_id)
            
            # Update the list of IDs in this chunk (store up to 100 IDs per chunk for analysis)
            if chunk_idx not in chunk_row_ids:
                chunk_row_ids[chunk_idx] = []
            if len(chunk_row_ids[chunk_idx]) < 100:
                chunk_row_ids[chunk_idx].append(row_id)
            
            # Log every 1000th row for debugging
            if row_id % 1000 == 0:
                logger.debug(f"Processed row with ID {row_id}, offset {row_offset}, assigned to chunk {chunk_idx}")
        
        # Create a monitoring hook for the cloudfetch module
        original_get_next_file = None
        chunk_download_counts = {}
        
        # Try to monkey patch the download manager to track chunk downloads
        try:
            from databricks.sql.cloudfetch.download_manager import ResultFileDownloadManager
            original_get_next_file = ResultFileDownloadManager.get_next_downloaded_file
            
            def patched_get_next_file(self, row_offset):
                # Call the original method
                result = original_get_next_file(self, row_offset)
                
                # Track which chunk this download is for
                if 'chunks' in globals():
                    chunk_idx = track_row_chunk(row_offset)
                    chunk_download_counts[chunk_idx] = chunk_download_counts.get(chunk_idx, 0) + 1
                    logger.debug(f"Download for row_offset {row_offset} (chunk {chunk_idx}), download count: {chunk_download_counts[chunk_idx]}")
                
                return result
            
            # Apply the monkey patch
            ResultFileDownloadManager.get_next_downloaded_file = patched_get_next_file
            logger.info("Successfully patched ResultFileDownloadManager for detailed monitoring")
        except Exception as e:
            logger.warning(f"Could not patch ResultFileDownloadManager: {e}")
            
        # Create a function to log the state of chunk downloads
        def log_chunk_download_state():
            logger.info("\n=== CHUNK DOWNLOAD STATE ===")
            for i in range(len(chunks)):
                download_count = chunk_download_counts.get(i, 0)
                expected_rows = chunks[i].get('row_count', 0)
                logger.info(f"Chunk {i}: {download_count} downloads for {expected_rows} expected rows")
        
        # Fetch first batch
        current_offset = 1  # Start at 1 because we already fetched the first row
        batch1 = cursor.fetchmany(batch_size)
        if batch1:
            batch1_ids = [row.id for row in batch1 if hasattr(row, 'id')]
            for row_id in batch1_ids:
                all_ids.add(row_id)
                update_chunk_tracking(row_id, current_offset)
                current_offset += 1
            
            logger.info(f"Batch 1: {len(batch1)} rows, ID range: {min(batch1_ids)} to {max(batch1_ids)}")
            # Log the download state after first batch
            log_chunk_download_state()
        
        # Fetch second batch
        batch2 = cursor.fetchmany(batch_size)
        if batch2:
            batch2_ids = [row.id for row in batch2 if hasattr(row, 'id')]
            for row_id in batch2_ids:
                all_ids.add(row_id)
                update_chunk_tracking(row_id, current_offset)
                current_offset += 1
                
            logger.info(f"Batch 2: {len(batch2)} rows, ID range: {min(batch2_ids)} to {max(batch2_ids)}")
            # Log the download state after second batch
            log_chunk_download_state()
        
        # Fetch remaining rows
        remaining = cursor.fetchall()
        if remaining:
            remaining_ids = [row.id for row in remaining if hasattr(row, 'id')]
            for row_id in remaining_ids:
                all_ids.add(row_id)
                update_chunk_tracking(row_id, current_offset)
                current_offset += 1
                
            logger.info(f"Remaining: {len(remaining)} rows, ID range: {min(remaining_ids)} to {max(remaining_ids) if remaining_ids else None}")
            # Log the download state after all rows
            log_chunk_download_state()
        
        # Check for completeness
        if all_ids:
            min_id = min(all_ids)
            max_id = max(all_ids)
            actual_count = len(all_ids)
            
            # Calculate the expected row count based on the manifest
            total_rows_in_manifest = sum(chunk.get('row_count', 0) for chunk in chunks) if 'chunks' in locals() else 0
            logger.info(f"Total rows reported in manifest: {total_rows_in_manifest}")
            
            # Log the last row of each chunk based on manifest
            logger.info("\n=== MANIFEST CHUNK BOUNDARIES ===")
            cum_rows = 0
            for i, chunk in enumerate(chunks):
                rows = chunk.get('row_count', 0)
                start_row = cum_rows + 1
                end_row = cum_rows + rows
                cum_rows += rows
                logger.info(f"Chunk {i}: rows {start_row} to {end_row} (count: {rows})")
            
            logger.info(f"Row ID range: {min_id} to {max_id}")
            logger.info(f"Requested row count: {requested_row_count}")
            logger.info(f"Actual unique IDs collected: {actual_count}")
            
            # First check if we got all the rows reported in the manifest
            if total_rows_in_manifest > 0 and actual_count == total_rows_in_manifest:
                logger.info("✅ All rows from the manifest were fetched correctly")
            elif total_rows_in_manifest > 0:
                logger.warning(f"⚠️ Manifest row count mismatch: manifest reports {total_rows_in_manifest}, got {actual_count}")
            
            # Then check if we got all the rows we requested
            if actual_count == requested_row_count:
                logger.info("✅ All requested rows were fetched")
            else:
                logger.warning(f"⚠️ Requested vs actual row count mismatch: requested {requested_row_count}, got {actual_count}")
            
            # Log detailed chunk analysis
            logger.info("\n=== DETAILED CHUNK ANALYSIS ===")
            for i in range(len(chunks)):
                expected_rows = chunks[i].get('row_count', 0)
                actual_rows = chunk_row_counts.get(i, 0)
                id_range = chunk_id_ranges.get(i, (None, None))
                
                logger.info(f"Chunk {i}: Expected {expected_rows} rows, Actually got {actual_rows} rows")
                if id_range[0] is not None:
                    logger.info(f"  ID range: {id_range[0]} to {id_range[1]}")
                    # Calculate the number of unique IDs in this range
                    unique_ids_in_range = len(set(range(id_range[0], id_range[1] + 1)) & all_ids)
                    logger.info(f"  Unique IDs in this range: {unique_ids_in_range}")
                    
                    # Show sample of actual IDs in this chunk
                    if i in chunk_row_ids and chunk_row_ids[i]:
                        sample_ids = chunk_row_ids[i][:20]  # Show first 20 IDs
                        logger.info(f"  Sample IDs: {sample_ids}")
                        
                        # Check for duplicate IDs within this chunk
                        if len(sample_ids) != len(set(sample_ids)):
                            logger.warning(f"  ⚠️ Duplicate IDs detected in chunk {i}")
                            
                        # Check for overlapping IDs with other chunks
                        for j in range(len(chunks)):
                            if i != j and j in chunk_row_ids and chunk_row_ids[j]:
                                overlap = set(chunk_row_ids[i]) & set(chunk_row_ids[j])
                                if overlap:
                                    logger.warning(f"  ⚠️ Chunk {i} has {len(overlap)} overlapping IDs with chunk {j}")
                                    logger.warning(f"  ⚠️ Sample overlapping IDs: {sorted(list(overlap))[:10]}")
                    
                    # For the last chunk, do more detailed analysis
                    if i == len(chunks) - 1:
                        logger.info("\n=== LAST CHUNK DETAILED ANALYSIS ===")
                        # Calculate how many IDs we should have based on the manifest
                        expected_last_id = sum(chunk.get('row_count', 0) for chunk in chunks)
                        logger.info(f"Expected last ID based on manifest: {expected_last_id}")
                        logger.info(f"Actual last ID: {max_id}")
                        
                        # Check if the last chunk's IDs are what we expect
                        expected_last_chunk_ids = set(range(max_id - expected_rows + 1, max_id + 1))
                        actual_last_chunk_ids = set(range(id_range[0], id_range[1] + 1)) & all_ids
                        logger.info(f"Expected last chunk to contain IDs from {max_id - expected_rows + 1} to {max_id}")
                        logger.info(f"Actually contains IDs from {min(actual_last_chunk_ids)} to {max(actual_last_chunk_ids)}")
                        
                        # Check if the number of rows matches what the manifest says
                        if expected_rows == len(actual_last_chunk_ids):
                            logger.info("✅ Last chunk has the expected number of rows")
                        else:
                            logger.warning(f"⚠️ Last chunk has {len(actual_last_chunk_ids)} rows, expected {expected_rows}")
                
                if expected_rows != actual_rows:
                    logger.warning(f"  ⚠️ Row count mismatch in chunk {i}: expected {expected_rows}, got {actual_rows}")
                    if actual_rows < expected_rows:
                        logger.warning(f"  ⚠️ Missing {expected_rows - actual_rows} rows in chunk {i}")
                    else:
                        logger.warning(f"  ⚠️ Extra {actual_rows - expected_rows} rows in chunk {i}")
                        
                        # If we have extra rows, try to determine where they came from
                        if actual_rows > expected_rows and id_range[0] is not None:
                            # Calculate the expected ID range for this chunk
                            chunk_start_offset = sum(chunks[j].get('row_count', 0) for j in range(i))
                            expected_start_id = chunk_start_offset + 1
                            expected_end_id = expected_start_id + expected_rows - 1
                            logger.info(f"  Expected ID range: {expected_start_id} to {expected_end_id}")
                            
                            # Check if the extra rows are from beyond the expected range
                            if id_range[1] > expected_end_id:
                                extra_ids = set(range(expected_end_id + 1, id_range[1] + 1)) & all_ids
                                logger.warning(f"  ⚠️ Extra rows include IDs from {min(extra_ids)} to {max(extra_ids)}")
                            
                            # Check if there are duplicate IDs
                            if len(set(range(id_range[0], id_range[1] + 1)) & all_ids) < actual_rows:
                                logger.warning("  ⚠️ There may be duplicate IDs in this chunk")
            
            # Calculate cumulative rows per chunk from manifest
            cumulative_rows = 0
            logger.info("\n=== CUMULATIVE ROW ANALYSIS ===")
            for i, chunk in enumerate(chunks):
                chunk_rows = chunk.get('row_count', 0)
                cumulative_rows += chunk_rows
                logger.info(f"After chunk {i}: Should have {cumulative_rows} rows (chunk has {chunk_rows})")
                
            # Check if IDs are sequential from 1 to max_id
            expected_ids = set(range(1, max_id + 1))
            if all_ids == expected_ids:
                logger.info("✅ All IDs are sequential as expected")
            else:
                missing = expected_ids - all_ids
                unexpected = all_ids - expected_ids
                if missing:
                    logger.warning(f"⚠️ Missing IDs in sequence: {sorted(missing)[:10]}... ({len(missing)} total)")
                    
                    # Analyze missing IDs to find patterns
                    missing_list = sorted(missing)
                    if len(missing_list) > 0:
                        # Check if missing IDs are consecutive
                        consecutive_ranges = []
                        current_range = [missing_list[0], missing_list[0]]
                        
                        for i in range(1, len(missing_list)):
                            if missing_list[i] == missing_list[i-1] + 1:
                                # Continue current range
                                current_range[1] = missing_list[i]
                            else:
                                # Start new range
                                consecutive_ranges.append(current_range)
                                current_range = [missing_list[i], missing_list[i]]
                        
                        # Add the last range
                        consecutive_ranges.append(current_range)
                        
                        # Log the ranges
                        logger.info("\n=== MISSING ID PATTERNS ===")
                        if len(consecutive_ranges) <= 10:
                            for r in consecutive_ranges:
                                if r[0] == r[1]:
                                    logger.info(f"Missing single ID: {r[0]}")
                                else:
                                    logger.info(f"Missing range: {r[0]} to {r[1]} ({r[1] - r[0] + 1} IDs)")
                        else:
                            logger.info(f"Found {len(consecutive_ranges)} separate ranges of missing IDs")
                            logger.info(f"First few ranges:")
                            for r in consecutive_ranges[:5]:
                                if r[0] == r[1]:
                                    logger.info(f"  Missing single ID: {r[0]}")
                                else:
                                    logger.info(f"  Missing range: {r[0]} to {r[1]} ({r[1] - r[0] + 1} IDs)")
                            logger.info(f"Last few ranges:")
                            for r in consecutive_ranges[-5:]:
                                if r[0] == r[1]:
                                    logger.info(f"  Missing single ID: {r[0]}")
                                else:
                                    logger.info(f"  Missing range: {r[0]} to {r[1]} ({r[1] - r[0] + 1} IDs)")
                        
                        # Check if missing IDs are at the end
                        if missing_list[0] > max_id - len(missing_list):
                            logger.info("⚠️ All missing IDs are at the end of the sequence")
                            
                if unexpected:
                    logger.warning(f"⚠️ Unexpected IDs: {sorted(unexpected)[:10]}... ({len(unexpected)} total)")
        
        # Now test with Arrow fetching
        logger.info("\nTesting Arrow fetching with a fresh cursor...")
        arrow_cursor = connection.cursor()
        
        # Reset chunk download counters for Arrow test
        chunk_download_counts = {}
        
        # Use the same query with the same expected row count
        arrow_cursor.execute(query)
        
        try:
            # Fetch all rows as Arrow
            logger.info("Fetching all rows as Arrow...")
            arrow_table = arrow_cursor.fetchall_arrow()
            logger.info(f"Arrow table num rows: {arrow_table.num_rows}")
            
            # Log the download state after Arrow fetch
            logger.info("\n=== ARROW FETCH CHUNK DOWNLOAD STATE ===")
            for i in range(len(chunks)):
                download_count = chunk_download_counts.get(i, 0)
                expected_rows = chunks[i].get('row_count', 0)
                logger.info(f"Chunk {i}: {download_count} downloads for {expected_rows} expected rows")
            
            # Check ID column
            if 'id' in arrow_table.column_names:
                id_column = arrow_table.column('id').to_pylist()
                arrow_ids = set(id_column)
                arrow_min_id = min(id_column) if id_column else 0
                arrow_max_id = max(id_column) if id_column else 0
                
                logger.info(f"Arrow result: Row ID range: {arrow_min_id} to {arrow_max_id}")
                logger.info(f"Arrow result: Requested row count: {requested_row_count}")
                logger.info(f"Arrow result: Actual unique IDs: {len(arrow_ids)}")
                
                # First check if we got all the rows reported in the manifest
                if total_rows_in_manifest > 0 and len(arrow_ids) == total_rows_in_manifest:
                    logger.info("✅ Arrow results: All rows from the manifest were fetched correctly")
                elif total_rows_in_manifest > 0:
                    logger.warning(f"⚠️ Arrow results: Manifest row count mismatch: manifest reports {total_rows_in_manifest}, got {len(arrow_ids)}")
                
                # Then check if we got all the rows we requested
                if len(arrow_ids) == requested_row_count:
                    logger.info("✅ Arrow results: All requested rows were fetched")
                else:
                    logger.warning(f"⚠️ Arrow results: Requested vs actual row count mismatch: requested {requested_row_count}, got {len(arrow_ids)}")
                
                # Check if IDs are sequential from 1 to max_id
                expected_ids = set(range(1, arrow_max_id + 1))
                if arrow_ids == expected_ids:
                    logger.info("✅ Arrow results: All IDs are sequential as expected")
                else:
                    missing = expected_ids - arrow_ids
                    unexpected = arrow_ids - expected_ids
                    if missing:
                        logger.warning(f"⚠️ Arrow results: Missing IDs in sequence: {sorted(missing)[:10]}... ({len(missing)} total)")
                        
                        # Analyze missing IDs to find patterns (similar to above)
                        missing_list = sorted(missing)
                        if len(missing_list) > 0:
                            # Check if missing IDs are consecutive
                            consecutive_ranges = []
                            current_range = [missing_list[0], missing_list[0]]
                            
                            for i in range(1, len(missing_list)):
                                if missing_list[i] == missing_list[i-1] + 1:
                                    # Continue current range
                                    current_range[1] = missing_list[i]
                                else:
                                    # Start new range
                                    consecutive_ranges.append(current_range)
                                    current_range = [missing_list[i], missing_list[i]]
                            
                            # Add the last range
                            consecutive_ranges.append(current_range)
                            
                            # Log the ranges
                            logger.info("\n=== ARROW MISSING ID PATTERNS ===")
                            if len(consecutive_ranges) <= 10:
                                for r in consecutive_ranges:
                                    if r[0] == r[1]:
                                        logger.info(f"Missing single ID: {r[0]}")
                                    else:
                                        logger.info(f"Missing range: {r[0]} to {r[1]} ({r[1] - r[0] + 1} IDs)")
                            else:
                                logger.info(f"Found {len(consecutive_ranges)} separate ranges of missing IDs")
                                logger.info(f"First few ranges:")
                                for r in consecutive_ranges[:5]:
                                    if r[0] == r[1]:
                                        logger.info(f"  Missing single ID: {r[0]}")
                                    else:
                                        logger.info(f"  Missing range: {r[0]} to {r[1]} ({r[1] - r[0] + 1} IDs)")
                                logger.info(f"Last few ranges:")
                                for r in consecutive_ranges[-5:]:
                                    if r[0] == r[1]:
                                        logger.info(f"  Missing single ID: {r[0]}")
                                    else:
                                        logger.info(f"  Missing range: {r[0]} to {r[1]} ({r[1] - r[0] + 1} IDs)")
                            
                            # Check if missing IDs are at the end
                            if missing_list[0] > arrow_max_id - len(missing_list):
                                logger.info("⚠️ All missing IDs are at the end of the sequence")
                    
                    if unexpected:
                        logger.warning(f"⚠️ Arrow results: Unexpected IDs: {sorted(unexpected)[:10]}... ({len(unexpected)} total)")
                
                # Compare with sequential fetch
                if all_ids == arrow_ids:
                    logger.info("✅ Arrow and sequential fetch results match exactly")
                else:
                    logger.warning("⚠️ Arrow and sequential fetch results differ")
                    only_in_arrow = arrow_ids - all_ids
                    only_in_sequential = all_ids - arrow_ids
                    if only_in_arrow:
                        logger.warning(f"IDs only in Arrow results: {sorted(list(only_in_arrow)[:10])}... ({len(only_in_arrow)} rows)")
                    if only_in_sequential:
                        logger.warning(f"IDs only in sequential fetch: {sorted(list(only_in_sequential)[:10])}... ({len(only_in_sequential)} rows)")
            
        except Exception as e:
            logger.error(f"Error during Arrow fetching: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Close cursors and connection
        cursor.close()
        if 'arrow_cursor' in locals():
            arrow_cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")

    except Exception as e:
        logger.error(f"Error during multi-chunk test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    logger.info("Multi-chunk result set test completed successfully")


if __name__ == "__main__":
    # Test result set implementation with JSON_ARRAY format and INLINE disposition
    # test_sea_result_set_json_array_inline()
    
    # Test result set implementation with multiple chunks
    test_multi_chunk_result_set()