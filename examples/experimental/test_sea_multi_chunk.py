"""
Test for SEA multi-chunk responses.

This script tests the SEA connector's ability to handle multi-chunk responses correctly.
It runs a query that generates large rows to force multiple chunks and verifies that
the correct number of rows are returned.
"""
import os
import sys
import logging
import time
import json
import csv
from pathlib import Path
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
    
    # Create output directory for test results
    output_dir = Path("test_results")
    output_dir.mkdir(exist_ok=True)
    
    # Files to store results
    rows_file = output_dir / "cloud_fetch_rows.csv"
    stats_file = output_dir / "cloud_fetch_stats.json"

    if not all([server_hostname, http_path, access_token]):
        logger.error("Missing required environment variables.")
        logger.error(
            "Please set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN."
        )
        return False

    try:
        # Create connection with cloud fetch enabled
        logger.info(
            "Creating connection for query execution with cloud fetch enabled"
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
        cursor = connection.cursor()
        query = f"""
        SELECT 
            id, 
            concat('value_', repeat('a', 10000)) as test_value
        FROM range(1, {requested_row_count} + 1) AS t(id)
        """
        
        logger.info(f"Executing query with cloud fetch to generate {requested_row_count} rows")
        start_time = time.time()
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        actual_row_count = len(rows)
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info(f"Query executed in {execution_time:.2f} seconds")
        logger.info(f"Requested {requested_row_count} rows, received {actual_row_count} rows")
        
        # Write rows to CSV file for inspection
        logger.info(f"Writing rows to {rows_file}")
        with open(rows_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'value_length'])  # Header
            
            # Extract IDs to check for duplicates and missing values
            row_ids = []
            for row in rows:
                row_id = row[0]
                value_length = len(row[1])
                writer.writerow([row_id, value_length])
                row_ids.append(row_id)
        
        # Verify row count
        success = actual_row_count == requested_row_count
        
        # Check for duplicate IDs
        unique_ids = set(row_ids)
        duplicate_count = len(row_ids) - len(unique_ids)
        
        # Check for missing IDs
        expected_ids = set(range(1, requested_row_count + 1))
        missing_ids = expected_ids - unique_ids
        extra_ids = unique_ids - expected_ids
        
        # Write statistics to JSON file
        stats = {
            "requested_row_count": requested_row_count,
            "actual_row_count": actual_row_count,
            "execution_time_seconds": execution_time,
            "duplicate_count": duplicate_count,
            "missing_ids_count": len(missing_ids),
            "extra_ids_count": len(extra_ids),
            "missing_ids": list(missing_ids)[:100] if missing_ids else [],  # Limit to first 100 for readability
            "extra_ids": list(extra_ids)[:100] if extra_ids else [],  # Limit to first 100 for readability
            "success": success and duplicate_count == 0 and len(missing_ids) == 0 and len(extra_ids) == 0
        }
        
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        # Log detailed results
        if duplicate_count > 0:
            logger.error(f"❌ FAILED: Found {duplicate_count} duplicate row IDs")
            success = False
        else:
            logger.info("✅ PASSED: No duplicate row IDs found")
            
        if missing_ids:
            logger.error(f"❌ FAILED: Missing {len(missing_ids)} expected row IDs")
            if len(missing_ids) <= 10:
                logger.error(f"Missing IDs: {sorted(list(missing_ids))}")
            success = False
        else:
            logger.info("✅ PASSED: All expected row IDs present")
            
        if extra_ids:
            logger.error(f"❌ FAILED: Found {len(extra_ids)} unexpected row IDs")
            if len(extra_ids) <= 10:
                logger.error(f"Extra IDs: {sorted(list(extra_ids))}")
            success = False
        else:
            logger.info("✅ PASSED: No unexpected row IDs found")
        
        if actual_row_count == requested_row_count:
            logger.info("✅ PASSED: Row count matches requested count")
        else:
            logger.error(f"❌ FAILED: Row count mismatch. Expected {requested_row_count}, got {actual_row_count}")
            success = False
        
        # Close resources
        cursor.close()
        connection.close()
        logger.info("Successfully closed SEA session")
        
        logger.info(f"Test results written to {rows_file} and {stats_file}")
        return success

    except Exception as e:
        logger.error(
            f"Error during SEA multi-chunk test with cloud fetch: {str(e)}"
        )
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
    requested_row_count = 5000
    
    if len(sys.argv) > 1:
        try:
            requested_row_count = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid row count: {sys.argv[1]}")
            logger.error("Please provide a valid integer for row count.")
            sys.exit(1)
    
    logger.info(f"Testing with {requested_row_count} rows")
    
    # Run the multi-chunk test with cloud fetch
    success = test_sea_multi_chunk_with_cloud_fetch(requested_row_count)
    
    # Report results
    if success:
        logger.info("✅ TEST PASSED: Multi-chunk cloud fetch test completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ TEST FAILED: Multi-chunk cloud fetch test encountered errors")
        sys.exit(1)


if __name__ == "__main__":
    main()