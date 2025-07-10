"""
Tests for SEA-related queue classes.

This module contains tests for the JsonQueue, SeaResultSetQueueFactory, and SeaCloudFetchQueue classes.
"""

import pytest
from unittest.mock import Mock, patch

from databricks.sql.backend.sea.queue import (
    JsonQueue,
    SeaResultSetQueueFactory,
    SeaCloudFetchQueue,
)
from databricks.sql.backend.sea.models.base import (
    ResultData,
    ResultManifest,
    ExternalLink,
)
from databricks.sql.backend.sea.utils.constants import ResultFormat
from databricks.sql.exc import ProgrammingError
from databricks.sql.types import SSLOptions


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
        assert queue.num_rows == len(sample_data)

    def test_init_with_none(self):
        """Test initialization with None data."""
        queue = JsonQueue(None)
        assert queue.data_array == []
        assert queue.cur_row_index == 0
        assert queue.num_rows == 0

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

    def test_next_n_rows_zero(self, sample_data):
        """Test fetching zero rows."""
        queue = JsonQueue(sample_data)
        result = queue.next_n_rows(0)
        assert result == []
        assert queue.cur_row_index == 0

    def test_remaining_rows(self, sample_data):
        """Test fetching all remaining rows."""
        queue = JsonQueue(sample_data)

        # Fetch some rows first
        queue.next_n_rows(2)

        # Now fetch remaining
        result = queue.remaining_rows()
        assert result == sample_data[2:]
        assert queue.cur_row_index == len(sample_data)

    def test_remaining_rows_all(self, sample_data):
        """Test fetching all remaining rows from the start."""
        queue = JsonQueue(sample_data)
        result = queue.remaining_rows()
        assert result == sample_data
        assert queue.cur_row_index == len(sample_data)

    def test_remaining_rows_empty(self, sample_data):
        """Test fetching remaining rows when none are left."""
        queue = JsonQueue(sample_data)

        # Fetch all rows first
        queue.next_n_rows(len(sample_data))

        # Now fetch remaining (should be empty)
        result = queue.remaining_rows()
        assert result == []
        assert queue.cur_row_index == len(sample_data)


class TestSeaResultSetQueueFactory:
    """Test suite for the SeaResultSetQueueFactory class."""

    @pytest.fixture
    def json_manifest(self):
        """Create a JSON manifest for testing."""
        return ResultManifest(
            format=ResultFormat.JSON_ARRAY.value,
            schema={},
            total_row_count=5,
            total_byte_count=1000,
            total_chunk_count=1,
        )

    @pytest.fixture
    def arrow_manifest(self):
        """Create an Arrow manifest for testing."""
        return ResultManifest(
            format=ResultFormat.ARROW_STREAM.value,
            schema={},
            total_row_count=5,
            total_byte_count=1000,
            total_chunk_count=1,
        )

    @pytest.fixture
    def invalid_manifest(self):
        """Create an invalid manifest for testing."""
        return ResultManifest(
            format="INVALID_FORMAT",
            schema={},
            total_row_count=5,
            total_byte_count=1000,
            total_chunk_count=1,
        )

    @pytest.fixture
    def sample_data(self):
        """Create sample result data."""
        return [
            ["value1", "1", "true"],
            ["value2", "2", "false"],
        ]

    @pytest.fixture
    def ssl_options(self):
        """Create SSL options for testing."""
        return SSLOptions(tls_verify=True)

    @pytest.fixture
    def mock_sea_client(self):
        """Create a mock SEA client."""
        client = Mock()
        client.max_download_threads = 10
        return client

    @pytest.fixture
    def description(self):
        """Create column descriptions."""
        return [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
            ("col3", "boolean", None, None, None, None, None),
        ]

    def test_build_queue_json_array(self, json_manifest, sample_data):
        """Test building a JSON array queue."""
        result_data = ResultData(data=sample_data)

        queue = SeaResultSetQueueFactory.build_queue(
            result_data=result_data,
            manifest=json_manifest,
            statement_id="test-statement",
            ssl_options=SSLOptions(),
            description=[],
            max_download_threads=10,
            sea_client=Mock(),
            lz4_compressed=False,
        )

        assert isinstance(queue, JsonQueue)
        assert queue.data_array == sample_data

    def test_build_queue_arrow_stream(
        self, arrow_manifest, ssl_options, mock_sea_client, description
    ):
        """Test building an Arrow stream queue."""
        external_links = [
            ExternalLink(
                external_link="https://example.com/data/chunk0",
                expiration="2025-07-03T05:51:18.118009",
                row_count=100,
                byte_count=1024,
                row_offset=0,
                chunk_index=0,
                next_chunk_index=1,
                http_headers={"Authorization": "Bearer token123"},
            )
        ]
        result_data = ResultData(data=None, external_links=external_links)

        with patch(
            "databricks.sql.backend.sea.queue.ResultFileDownloadManager"
        ), patch.object(SeaCloudFetchQueue, "_create_next_table", return_value=None):
            queue = SeaResultSetQueueFactory.build_queue(
                result_data=result_data,
                manifest=arrow_manifest,
                statement_id="test-statement",
                ssl_options=ssl_options,
                description=description,
                max_download_threads=10,
                sea_client=mock_sea_client,
                lz4_compressed=False,
            )

        assert isinstance(queue, SeaCloudFetchQueue)

    def test_build_queue_invalid_format(self, invalid_manifest):
        """Test building a queue with invalid format."""
        result_data = ResultData(data=[])

        with pytest.raises(ProgrammingError, match="Invalid result format"):
            SeaResultSetQueueFactory.build_queue(
                result_data=result_data,
                manifest=invalid_manifest,
                statement_id="test-statement",
                ssl_options=SSLOptions(),
                description=[],
                max_download_threads=10,
                sea_client=Mock(),
                lz4_compressed=False,
            )


class TestSeaCloudFetchQueue:
    """Test suite for the SeaCloudFetchQueue class."""

    @pytest.fixture
    def ssl_options(self):
        """Create SSL options for testing."""
        return SSLOptions(tls_verify=True)

    @pytest.fixture
    def mock_sea_client(self):
        """Create a mock SEA client."""
        client = Mock()
        client.max_download_threads = 10
        return client

    @pytest.fixture
    def description(self):
        """Create column descriptions."""
        return [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
            ("col3", "boolean", None, None, None, None, None),
        ]

    @pytest.fixture
    def sample_external_link(self):
        """Create a sample external link."""
        return ExternalLink(
            external_link="https://example.com/data/chunk0",
            expiration="2025-07-03T05:51:18.118009",
            row_count=100,
            byte_count=1024,
            row_offset=0,
            chunk_index=0,
            next_chunk_index=1,
            http_headers={"Authorization": "Bearer token123"},
        )

    @pytest.fixture
    def sample_external_link_no_headers(self):
        """Create a sample external link without headers."""
        return ExternalLink(
            external_link="https://example.com/data/chunk0",
            expiration="2025-07-03T05:51:18.118009",
            row_count=100,
            byte_count=1024,
            row_offset=0,
            chunk_index=0,
            next_chunk_index=1,
            http_headers=None,
        )

    @patch("databricks.sql.backend.sea.queue.ResultFileDownloadManager")
    @patch("databricks.sql.backend.sea.queue.logger")
    def test_init_no_initial_links(
        self,
        mock_logger,
        mock_download_manager_class,
        mock_sea_client,
        ssl_options,
        description,
    ):
        """Test initialization with no initial links."""
        # Create a queue with empty initial links
        queue = SeaCloudFetchQueue(
            initial_links=[],
            max_download_threads=5,
            ssl_options=ssl_options,
            sea_client=mock_sea_client,
            statement_id="test-statement-123",
            total_chunk_count=0,
            lz4_compressed=False,
            description=description,
        )

        # Verify debug message was logged
        mock_logger.debug.assert_called_with(
            "SeaCloudFetchQueue: Initialize CloudFetch loader for statement {}, total chunks: {}".format(
                "test-statement-123", 0
            )
        )

        # Verify download manager wasn't created
        mock_download_manager_class.assert_not_called()

        # Verify attributes
        assert queue._statement_id == "test-statement-123"
        assert (
            not hasattr(queue, "_current_chunk_link")
            or queue._current_chunk_link is None
        )
