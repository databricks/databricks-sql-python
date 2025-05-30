import os
import sys
import logging
from databricks.sql.client import Connection

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
        logger.error("Please set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN.")
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
            user_agent_entry="SEA-Test-Client" # add custom user agent
        )
        
        logger.info(f"Successfully opened SEA session with ID: {connection.get_session_id_hex()}")
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
    test_sea_session()