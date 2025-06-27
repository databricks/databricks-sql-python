"""
Tests for SEA-related queue classes in utils.py.

This module contains tests for the JsonQueue and SeaResultSetQueueFactory classes.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch

from databricks.sql.utils import JsonQueue, SeaResultSetQueueFactory
from databricks.sql.backend.sea.models.base import ResultData, ResultManifest


class TestJsonQueue:
    """Test suite for the JsonQueue class."""

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        return [
            ["value1", 1, True],
            ["value2", 2, False],
            ["value3", 3, True],
            ["value4", 4, False],
            ["value5", 5, True],
        ]

    def test_init(self, sample_data):
        """Test initialization of JsonQueue."""
        queue = JsonQueue(sample_data)
        assert queue.data_array == sample_data
        assert queue.cur_row_index == 0
        assert queue.n_valid_rows == len(sample_data)

    def test_next_n_rows_partial(self, sample_data):
        """Test fetching a subset of rows."""
        queue = JsonQueue(sample_data)
        result = queue.next_n_rows(2)
        assert result == sample_data[:2]
        assert queue.cur_row_index == 2

    def test_next_n_rows_all(self, sample_data):
        """Test fetching all rows."""
        queue = JsonQueue(sample_data)
        result = queue.next_n_rows(len(sample_data))
        assert result == sample_data
        assert queue.cur_row_index == len(sample_data)

    def test_next_n_rows_more_than_available(self, sample_data):
        """Test fetching more rows than available."""
        queue = JsonQueue(sample_data)
        result = queue.next_n_rows(len(sample_data) + 10)
        assert result == sample_data
        assert queue.cur_row_index == len(sample_data)

    def test_next_n_rows_after_partial(self, sample_data):
        """Test fetching rows after a partial fetch."""
        queue = JsonQueue(sample_data)
        queue.next_n_rows(2)  # Fetch first 2 rows
        result = queue.next_n_rows(2)  # Fetch next 2 rows
        assert result == sample_data[2:4]
        assert queue.cur_row_index == 4

    def test_remaining_rows_all(self, sample_data):
        """Test fetching all remaining rows at once."""
        queue = JsonQueue(sample_data)
        result = queue.remaining_rows()
        assert result == sample_data
        assert queue.cur_row_index == len(sample_data)

    def test_remaining_rows_after_partial(self, sample_data):
        """Test fetching remaining rows after a partial fetch."""
        queue = JsonQueue(sample_data)
        queue.next_n_rows(2)  # Fetch first 2 rows
        result = queue.remaining_rows()  # Fetch remaining rows
        assert result == sample_data[2:]
        assert queue.cur_row_index == len(sample_data)

    def test_empty_data(self):
        """Test with empty data array."""
        queue = JsonQueue([])
        assert queue.next_n_rows(10) == []
        assert queue.remaining_rows() == []
        assert queue.cur_row_index == 0
        assert queue.n_valid_rows == 0


class TestSeaResultSetQueueFactory:
    """Test suite for the SeaResultSetQueueFactory class."""

    @pytest.fixture
    def mock_sea_client(self):
        """Create a mock SEA client."""
        client = Mock()
        client.max_download_threads = 10
        return client

    @pytest.fixture
    def mock_description(self):
        """Create a mock column description."""
        return [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
            ("col3", "boolean", None, None, None, None, None),
        ]

    def test_build_queue_with_inline_data(self, mock_sea_client, mock_description):
        """Test building a queue with inline JSON data."""
        # Create sample data for inline JSON result
        data = [
            ["value1", "1", "true"],
            ["value2", "2", "false"],
        ]

        # Create a ResultData object with inline data
        result_data = ResultData(data=data, external_links=None, row_count=len(data))

        # Create a manifest (not used for inline data)
        manifest = None

        # Build the queue
        queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            manifest,
            "test-statement-123",
            description=mock_description,
            sea_client=mock_sea_client,
        )

        # Verify the queue is a JsonQueue with the correct data
        assert isinstance(queue, JsonQueue)
        assert queue.data_array == data
        assert queue.n_valid_rows == len(data)

    def test_build_queue_with_empty_data(self, mock_sea_client, mock_description):
        """Test building a queue with empty data."""
        # Create a ResultData object with no data
        result_data = ResultData(data=None, external_links=None, row_count=0)

        # Build the queue
        queue = SeaResultSetQueueFactory.build_queue(
            result_data,
            None,
            "test-statement-123",
            description=mock_description,
            sea_client=mock_sea_client,
        )

        # Verify the queue is a JsonQueue with empty data
        assert isinstance(queue, JsonQueue)
        assert queue.data_array == []
        assert queue.n_valid_rows == 0

    def test_build_queue_with_external_links(self, mock_sea_client, mock_description):
        """Test building a queue with external links raises NotImplementedError."""
        # Create a ResultData object with external links
        result_data = ResultData(
            data=None, external_links=["link1", "link2"], row_count=10
        )

        # Verify that NotImplementedError is raised
        with pytest.raises(
            NotImplementedError,
            match="EXTERNAL_LINKS disposition is not implemented for SEA backend",
        ):
            SeaResultSetQueueFactory.build_queue(
                result_data,
                None,
                "test-statement-123",
                description=mock_description,
                sea_client=mock_sea_client,
            )
