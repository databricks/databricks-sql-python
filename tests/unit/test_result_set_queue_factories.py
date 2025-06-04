"""
Tests for the ResultSetQueueFactory classes.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
from typing import Dict, List, Any, Optional

# Add the necessary path to import the modules
sys.path.append("/home/varun.edachali/conn/databricks-sql-python/src")

try:
    import pyarrow
except ImportError:
    pyarrow = None

from databricks.sql.utils import (
    ThriftResultSetQueueFactory,
    SeaResultSetQueueFactory,
    JsonQueue,
    ArrowQueue,
    ColumnQueue,
    CloudFetchQueue,
)
from databricks.sql.thrift_api.TCLIService.ttypes import TSparkRowSetType, TRowSet
from databricks.sql.backend.models import ResultData


class TestResultSetQueueFactories(unittest.TestCase):
    """Tests for the ThriftResultSetQueueFactory and SeaResultSetQueueFactory classes."""

    def test_sea_result_set_queue_factory_with_data(self):
        """Test SeaResultSetQueueFactory with data."""
        # Create a mock ResultData with data
        result_data = MagicMock(spec=ResultData)
        result_data.data = [[1, "Alice"], [2, "Bob"]]
        result_data.external_links = None

        # Build queue
        queue = SeaResultSetQueueFactory.build_queue(result_data)

        # Verify queue type
        self.assertIsInstance(queue, JsonQueue)
        self.assertEqual(queue.n_valid_rows, 2)
        self.assertEqual(queue.data_array, [[1, "Alice"], [2, "Bob"]])

    def test_sea_result_set_queue_factory_with_empty_data(self):
        """Test SeaResultSetQueueFactory with empty data."""
        # Create a mock ResultData with empty data
        result_data = MagicMock(spec=ResultData)
        result_data.data = []
        result_data.external_links = None

        with self.assertRaises(AssertionError):
            SeaResultSetQueueFactory.build_queue(result_data)

    def test_sea_result_set_queue_factory_with_external_links(self):
        """Test SeaResultSetQueueFactory with external links."""
        # Create a mock ResultData with external links
        result_data = MagicMock(spec=ResultData)
        result_data.data = None
        result_data.external_links = [{"url": "https://example.com/data"}]

        # Verify NotImplementedError is raised
        with self.assertRaises(NotImplementedError):
            SeaResultSetQueueFactory.build_queue(result_data)

    def test_sea_result_set_queue_factory_with_no_data(self):
        """Test SeaResultSetQueueFactory with no data."""
        # Create a mock ResultData with no data
        result_data = MagicMock(spec=ResultData)
        result_data.data = None
        result_data.external_links = None

        with self.assertRaises(AssertionError):
            SeaResultSetQueueFactory.build_queue(result_data)


if __name__ == "__main__":
    unittest.main()
