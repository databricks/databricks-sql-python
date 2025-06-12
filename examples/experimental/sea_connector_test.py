"""
Main script to run all SEA connector tests.

This script runs all the individual test modules and displays
a summary of test results with visual indicators.
"""
import os
import sys
import logging
import subprocess
from typing import List, Tuple

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TEST_MODULES = [
    "test_sea_session",
    "test_sea_sync_query",
    "test_sea_async_query",
    "test_sea_metadata",
]


def run_test_module(module_name: str) -> bool:
    """Run a test module and return success status."""
    module_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "tests", f"{module_name}.py"
    )

    # Simply run the module as a script - each module handles its own test execution
    result = subprocess.run(
        [sys.executable, module_path], capture_output=True, text=True
    )

    # Log the output from the test module
    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            logger.info(line)

    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            logger.error(line)

    return result.returncode == 0


def run_tests() -> List[Tuple[str, bool]]:
    """Run all tests and return results."""
    results = []

    for module_name in TEST_MODULES:
        try:
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Running test: {module_name}")
            logger.info(f"{'-' * 50}")

            success = run_test_module(module_name)
            results.append((module_name, success))

            status = "✅ PASSED" if success else "❌ FAILED"
            logger.info(f"Test {module_name}: {status}")

        except Exception as e:
            logger.error(f"Error loading or running test {module_name}: {str(e)}")
            import traceback

            logger.error(traceback.format_exc())
            results.append((module_name, False))

    return results


def print_summary(results: List[Tuple[str, bool]]) -> None:
    """Print a summary of test results."""
    logger.info(f"\n{'=' * 50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'-' * 50}")

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for module_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        logger.info(f"{status} - {module_name}")

    logger.info(f"{'-' * 50}")
    logger.info(f"Total: {total} | Passed: {passed} | Failed: {total - passed}")
    logger.info(f"{'=' * 50}")

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
