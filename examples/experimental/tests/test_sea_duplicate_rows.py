"""
Test specifically for the duplicate rows issue in SEA.
"""
import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sea_duplicate_rows():
    """
    Test to identify duplicate rows in SEA results.
    
    This test executes a query that should return a specific number of rows,
    and checks if the actual number of rows matches the expected count.
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
        # Test with cloud fetch enabled
        logger.info("=== Testing with cloud fetch ENABLED ===")
        test_with_cloud_fetch(server_hostname, http_path, access_token, catalog)
        
        return True

    except Exception as e:
        logger.error(f"Error during SEA duplicate rows test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_with_cloud_fetch(server_hostname, http_path, access_token, catalog):
    """Test for duplicate rows with cloud fetch enabled."""
    # Create connection with cloud fetch enabled
    connection = Connection(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema="default",
        use_sea=True,
        user_agent_entry="SEA-Test-Client",
        use_cloud_fetch=True,
        arraysize=10000,  # Set a large arraysize to see if it affects the results
    )

    logger.info(f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}")

    # Focus on the problematic case
    count = 10000
    cursor = connection.cursor()
    # Using the same query as in the original test that showed the issue
    query = f""" 
    SELECT 
        id, 
        concat('value_', repeat('a', 10000)) as test_value
        FROM range(1, {count} + 1) AS t(id)
    """
    
    logger.info(f"Executing query for {count} rows with cloud fetch enabled")
    cursor.execute(query)
    
    rows = cursor.fetchall()
    logger.info(f"Requested {count} rows, retrieved {len(rows)} rows")
    
    # Check for duplicate rows
    row_ids = [row[0] for row in rows]
    unique_ids = set(row_ids)
    logger.info(f"Number of unique IDs: {len(unique_ids)}")
    
    if len(rows) != len(unique_ids):
        logger.info("DUPLICATE ROWS DETECTED!")
        
        # Analyze the distribution of IDs
        logger.info("Analyzing ID distribution...")
        
        # Create a sorted list of all IDs to see if there are patterns
        sorted_ids = sorted(row_ids)
        
        # Check for missing IDs
        expected_ids = set(range(1, count + 1))
        missing_ids = expected_ids - set(sorted_ids)
        logger.info(f"Missing IDs count: {len(missing_ids)}")
        if missing_ids:
            logger.info(f"Sample of missing IDs: {sorted(list(missing_ids))[:20]}")
        
        # Find duplicates
        id_counts = {}
        for id_val in row_ids:
            if id_val in id_counts:
                id_counts[id_val] += 1
            else:
                id_counts[id_val] = 1
        
        duplicates = {id_val: count for id_val, count in id_counts.items() if count > 1}
        logger.info(f"Number of duplicate IDs: {len(duplicates)}")
        logger.info(f"Sample of duplicate IDs: {list(duplicates.items())[:20]}")
        
        # Check if duplicates occur at specific positions
        logger.info("Checking positions of duplicates...")
        
        # Find the positions of each ID in the result set
        id_positions = {}
        for i, row_id in enumerate(row_ids):
            if row_id in id_positions:
                id_positions[row_id].append(i)
            else:
                id_positions[row_id] = [i]
        
        # Look at positions of duplicates
        duplicate_positions = {id_val: positions for id_val, positions in id_positions.items() if len(positions) > 1}
        
        # Check the distance between duplicates
        logger.info("Analyzing distance between duplicate occurrences...")
        distances = []
        for id_val, positions in duplicate_positions.items():
            for i in range(1, len(positions)):
                distances.append(positions[i] - positions[i-1])
        
        if distances:
            avg_distance = sum(distances) / len(distances)
            logger.info(f"Average distance between duplicates: {avg_distance}")
            logger.info(f"Min distance: {min(distances)}, Max distance: {max(distances)}")
            
            # Count distances by frequency
            distance_counts = {}
            for d in distances:
                if d in distance_counts:
                    distance_counts[d] += 1
                else:
                    distance_counts[d] = 1
            
            # Show most common distances
            sorted_distances = sorted(distance_counts.items(), key=lambda x: x[1], reverse=True)
            logger.info(f"Most common distances: {sorted_distances[:10]}")
        
        # Look for patterns in chunk boundaries
        logger.info("Checking for patterns at chunk boundaries...")
        
        # Get the approximate chunk sizes from the logs
        chunk_sizes = [2187, 2187, 2188, 2187, 2187]  # Based on previous logs
        
        chunk_start = 0
        for i, size in enumerate(chunk_sizes):
            chunk_end = chunk_start + size
            chunk_ids = row_ids[chunk_start:chunk_end]
            unique_chunk_ids = set(chunk_ids)
            
            logger.info(f"Chunk {i}: rows {chunk_start}-{chunk_end-1}, size {size}, unique IDs: {len(unique_chunk_ids)}")
            
            # Check for overlap with next chunk
            if i < len(chunk_sizes) - 1:
                next_chunk_start = chunk_end
                next_chunk_end = next_chunk_start + chunk_sizes[i+1]
                if next_chunk_end > len(row_ids):
                    next_chunk_end = len(row_ids)
                    
                next_chunk_ids = row_ids[next_chunk_start:next_chunk_end]
                
                # Check for IDs that appear in both chunks
                current_ids_set = set(chunk_ids)
                next_ids_set = set(next_chunk_ids)
                overlap = current_ids_set.intersection(next_ids_set)
                
                if overlap:
                    logger.info(f"Overlap between chunks {i} and {i+1}: {len(overlap)} IDs")
                    logger.info(f"Sample overlapping IDs: {sorted(list(overlap))[:10]}")
                    
                    # Check if the overlapping IDs are at the boundaries
                    end_of_current = chunk_ids[-10:]
                    start_of_next = next_chunk_ids[:10]
                    
                    logger.info(f"End of chunk {i}: {end_of_current}")
                    logger.info(f"Start of chunk {i+1}: {start_of_next}")
            
            chunk_start = chunk_end
    
    cursor.close()
    connection.close()
    logger.info("Successfully closed SEA session with cloud fetch enabled")


if __name__ == "__main__":
    success = test_sea_duplicate_rows()
    sys.exit(0 if success else 1)