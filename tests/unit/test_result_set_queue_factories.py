"""
Tests for the ResultSetQueueFactory classes.
"""

import unittest
from unittest.mock import MagicMock

from databricks.sql.utils import (
    SeaResultSetQueueFactory,
    JsonQueue,
)
from databricks.sql.backend.models.base import ResultData, ResultManifest


class TestResultSetQueueFactories(unittest.TestCase):
    """Tests for the SeaResultSetQueueFactory classes."""

    def test_sea_result_set_queue_factory_with_data(self):
        """Test SeaResultSetQueueFactory with data."""
        # Create a mock ResultData with data
        result_data = MagicMock(spec=ResultData)
        result_data.data = [[1, "Alice"], [2, "Bob"]]
        result_data.external_links = None
        
        # Create a mock manifest
        manifest = MagicMock(spec=ResultManifest)
        manifest.format = "JSON_ARRAY"
        manifest.total_chunk_count = 1

        # Build queue
        queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            manifest,
            "test-statement-id"
        )

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
        
        # Create a mock manifest
        manifest = MagicMock(spec=ResultManifest)
        manifest.format = "JSON_ARRAY"
        manifest.total_chunk_count = 1

        # Build queue
        queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            manifest,
            "test-statement-id"
        )

        # Verify queue type and properties
        self.assertIsInstance(queue, JsonQueue)
        self.assertEqual(queue.n_valid_rows, 0)
        self.assertEqual(queue.data_array, [])

    def test_sea_result_set_queue_factory_with_external_links(self):
        """Test SeaResultSetQueueFactory with external links."""
        # Create a mock ResultData with external links
        result_data = MagicMock(spec=ResultData)
        result_data.data = None
        result_data.external_links = [MagicMock()]
        
        # Create a mock manifest
        manifest = MagicMock(spec=ResultManifest)
        manifest.format = "ARROW_STREAM"
        manifest.total_chunk_count = 1

        # Verify ValueError is raised when required arguments are missing
        with self.assertRaises(ValueError):
            SeaResultSetQueueFactory.build_queue(
                result_data,
                manifest,
                "test-statement-id"
            )

    def test_sea_result_set_queue_factory_with_no_data(self):
        """Test SeaResultSetQueueFactory with no data."""
        # Create a mock ResultData with no data
        result_data = MagicMock(spec=ResultData)
        result_data.data = None
        result_data.external_links = None
        
        # Create a mock manifest
        manifest = MagicMock(spec=ResultManifest)
        manifest.format = "JSON_ARRAY"
        manifest.total_chunk_count = 1

        # Build queue
        queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            manifest,
            "test-statement-id"
        )

        # Verify queue type and properties
        self.assertIsInstance(queue, JsonQueue)
        self.assertEqual(queue.n_valid_rows, 0)
        self.assertEqual(queue.data_array, [])


if __name__ == "__main__":
    unittest.main()
