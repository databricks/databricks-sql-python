"""
Main script to run all SEA connector tests.

This script imports and runs all the individual test modules and displays
a summary of test results with visual indicators.
"""
import os
import sys
import logging
import importlib.util
from typing import Dict, Callable, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define test modules and their main test functions
TEST_MODULES = [
    "test_sea_session",
    "test_sea_sync_query",
    "test_sea_async_query",
    "test_sea_metadata",
]


def load_test_function(module_name: str) -> Callable:
    """Load a test function from a module."""
    module_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "tests", f"{module_name}.py"
    )

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Get the main test function (assuming it starts with "test_")
    for name in dir(module):
        if name.startswith("test_") and callable(getattr(module, name)):
            # For sync and async query modules, we want the main function that runs both tests
            if name == f"test_sea_{module_name.replace('test_sea_', '')}_exec":
                return getattr(module, name)

    # Fallback to the first test function found
    for name in dir(module):
        if name.startswith("test_") and callable(getattr(module, name)):
            return getattr(module, name)

    raise ValueError(f"No test function found in module {module_name}")


def run_tests() -> List[Tuple[str, bool]]:
    """Run all tests and return results."""
    results = []

    for module_name in TEST_MODULES:
        try:
            test_func = load_test_function(module_name)
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Running test: {module_name}")
            logger.info(f"{'-' * 50}")

            success = test_func()
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


if __name__ == "__main__":
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

    # Run all tests
    results = run_tests()

    # Print summary
    print_summary(results)

    # Exit with appropriate status code
    all_passed = all(success for _, success in results)
    sys.exit(0 if all_passed else 1)
