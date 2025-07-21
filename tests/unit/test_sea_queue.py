"""
Tests for SEA-related queue classes.

This module contains tests for the JsonQueue, SeaResultSetQueueFactory, and SeaCloudFetchQueue classes.
It also tests the Hybrid disposition which can create either ArrowQueue or SeaCloudFetchQueue based on
whether attachment is set.
"""

import pytest
from unittest.mock import Mock, patch

from databricks.sql.backend.sea.queue import (
    JsonQueue,
    LinkFetcher,
    SeaResultSetQueueFactory,
    SeaCloudFetchQueue,
)
from databricks.sql.backend.sea.models.base import (
    ResultData,
    ResultManifest,
    ExternalLink,
)
from databricks.sql.backend.sea.utils.constants import ResultFormat
from databricks.sql.exc import ProgrammingError, ServerOperationError
from databricks.sql.types import SSLOptions
from databricks.sql.utils import ArrowQueue
import threading
import time

try:
    import pyarrow as pa
except ImportError:
    pa = None


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

    @pytest.mark.skipif(pa is None, reason="pyarrow not installed")
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

    def test_convert_to_thrift_link(self, sample_external_link):
        """Test conversion of ExternalLink to TSparkArrowResultLink."""
        # Call the method directly
        result = LinkFetcher._convert_to_thrift_link(sample_external_link)

        # Verify the conversion
        assert result.fileLink == sample_external_link.external_link
        assert result.rowCount == sample_external_link.row_count
        assert result.bytesNum == sample_external_link.byte_count
        assert result.startRowOffset == sample_external_link.row_offset
        assert result.httpHeaders == sample_external_link.http_headers

    def test_convert_to_thrift_link_no_headers(self, sample_external_link_no_headers):
        """Test conversion of ExternalLink with no headers to TSparkArrowResultLink."""
        # Call the method directly
        result = LinkFetcher._convert_to_thrift_link(sample_external_link_no_headers)

        # Verify the conversion
        assert result.fileLink == sample_external_link_no_headers.external_link
        assert result.rowCount == sample_external_link_no_headers.row_count
        assert result.bytesNum == sample_external_link_no_headers.byte_count
        assert result.startRowOffset == sample_external_link_no_headers.row_offset
        assert result.httpHeaders == {}

    @patch("databricks.sql.backend.sea.queue.ResultFileDownloadManager")
    @patch("databricks.sql.backend.sea.queue.logger")
    @pytest.mark.skipif(pa is None, reason="pyarrow not installed")
    def test_init_with_valid_initial_link(
        self,
        mock_logger,
        mock_download_manager_class,
        mock_sea_client,
        ssl_options,
        description,
        sample_external_link,
    ):
        """Test initialization with valid initial link."""
        # Create a queue with valid initial link
        with patch.object(SeaCloudFetchQueue, "_create_next_table", return_value=None):
            queue = SeaCloudFetchQueue(
                result_data=ResultData(external_links=[sample_external_link]),
                max_download_threads=5,
                ssl_options=ssl_options,
                sea_client=mock_sea_client,
                statement_id="test-statement-123",
                total_chunk_count=1,
                lz4_compressed=False,
                description=description,
            )

        # Verify attributes
        assert queue._current_chunk_index == 0
        assert queue.link_fetcher is not None

    @patch("databricks.sql.backend.sea.queue.ResultFileDownloadManager")
    @patch("databricks.sql.backend.sea.queue.logger")
    @pytest.mark.skipif(pa is None, reason="pyarrow not installed")
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
            result_data=ResultData(external_links=[]),
            max_download_threads=5,
            ssl_options=ssl_options,
            sea_client=mock_sea_client,
            statement_id="test-statement-123",
            total_chunk_count=0,
            lz4_compressed=False,
            description=description,
        )
        assert queue.table == pa.Table.from_pydict({})

    @patch("databricks.sql.backend.sea.queue.logger")
    def test_create_next_table_success(self, mock_logger):
        """Test _create_next_table with successful table creation."""
        # Create a queue instance without initializing
        queue = Mock(spec=SeaCloudFetchQueue)
        queue._current_chunk_index = 0
        queue.download_manager = Mock()
        queue.link_fetcher = Mock()

        # Mock the dependencies
        mock_table = Mock()
        mock_chunk_link = Mock()
        queue.link_fetcher.get_chunk_link = Mock(return_value=mock_chunk_link)
        queue._create_table_at_offset = Mock(return_value=mock_table)

        # Call the method directly
        SeaCloudFetchQueue._create_next_table(queue)

        # Verify the chunk index was incremented
        assert queue._current_chunk_index == 1

        # Verify the chunk link was retrieved
        queue.link_fetcher.get_chunk_link.assert_called_once_with(0)

        # Verify the table was created from the link
        queue._create_table_at_offset.assert_called_once_with(
            mock_chunk_link.row_offset
        )


class TestHybridDisposition:
    """Test suite for the Hybrid disposition handling in SeaResultSetQueueFactory."""

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
    def description(self):
        """Create column descriptions."""
        return [
            ("col1", "string", None, None, None, None, None),
            ("col2", "int", None, None, None, None, None),
            ("col3", "boolean", None, None, None, None, None),
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

    @patch("databricks.sql.backend.sea.queue.create_arrow_table_from_arrow_file")
    def test_hybrid_disposition_with_attachment(
        self,
        mock_create_table,
        arrow_manifest,
        description,
        ssl_options,
        mock_sea_client,
    ):
        """Test that ArrowQueue is created when attachment is present."""
        # Create mock arrow table
        mock_arrow_table = Mock()
        mock_arrow_table.num_rows = 5
        mock_create_table.return_value = mock_arrow_table

        # Create result data with attachment
        attachment_data = b"mock_arrow_data"
        result_data = ResultData(attachment=attachment_data)

        # Build queue
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

        # Verify ArrowQueue was created
        assert isinstance(queue, ArrowQueue)
        mock_create_table.assert_called_once_with(attachment_data, description)

    @patch("databricks.sql.backend.sea.queue.ResultFileDownloadManager")
    @patch.object(SeaCloudFetchQueue, "_create_next_table", return_value=None)
    @pytest.mark.skipif(pa is None, reason="pyarrow not installed")
    def test_hybrid_disposition_with_external_links(
        self,
        mock_create_table,
        mock_download_manager,
        arrow_manifest,
        description,
        ssl_options,
        mock_sea_client,
    ):
        """Test that SeaCloudFetchQueue is created when attachment is None but external links are present."""
        # Create external links
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

        # Create result data with external links but no attachment
        result_data = ResultData(external_links=external_links, attachment=None)

        # Build queue
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

        # Verify SeaCloudFetchQueue was created
        assert isinstance(queue, SeaCloudFetchQueue)
        mock_create_table.assert_called_once()

    @patch("databricks.sql.backend.sea.queue.ResultSetDownloadHandler._decompress_data")
    @patch("databricks.sql.backend.sea.queue.create_arrow_table_from_arrow_file")
    def test_hybrid_disposition_with_compressed_attachment(
        self,
        mock_create_table,
        mock_decompress,
        arrow_manifest,
        description,
        ssl_options,
        mock_sea_client,
    ):
        """Test that ArrowQueue is created with decompressed data when attachment is present and lz4_compressed is True."""
        # Create mock arrow table
        mock_arrow_table = Mock()
        mock_arrow_table.num_rows = 5
        mock_create_table.return_value = mock_arrow_table

        # Setup decompression mock
        compressed_data = b"compressed_data"
        decompressed_data = b"decompressed_data"
        mock_decompress.return_value = decompressed_data

        # Create result data with attachment
        result_data = ResultData(attachment=compressed_data)

        # Build queue with lz4_compressed=True
        queue = SeaResultSetQueueFactory.build_queue(
            result_data=result_data,
            manifest=arrow_manifest,
            statement_id="test-statement",
            ssl_options=ssl_options,
            description=description,
            max_download_threads=10,
            sea_client=mock_sea_client,
            lz4_compressed=True,
        )

        # Verify ArrowQueue was created with decompressed data
        assert isinstance(queue, ArrowQueue)
        mock_decompress.assert_called_once_with(compressed_data)
        mock_create_table.assert_called_once_with(decompressed_data, description)


class TestLinkFetcher:
    """Unit tests for the LinkFetcher helper class."""

    @pytest.fixture
    def sample_links(self):
        """Provide a pair of ExternalLink objects forming two sequential chunks."""
        link0 = ExternalLink(
            external_link="https://example.com/data/chunk0",
            expiration="2030-01-01T00:00:00.000000",
            row_count=100,
            byte_count=1024,
            row_offset=0,
            chunk_index=0,
            next_chunk_index=1,
            http_headers={"Authorization": "Bearer token0"},
        )

        link1 = ExternalLink(
            external_link="https://example.com/data/chunk1",
            expiration="2030-01-01T00:00:00.000000",
            row_count=100,
            byte_count=1024,
            row_offset=100,
            chunk_index=1,
            next_chunk_index=None,
            http_headers={"Authorization": "Bearer token1"},
        )

        return link0, link1

    def _create_fetcher(
        self,
        initial_links,
        backend_mock=None,
        download_manager_mock=None,
        total_chunk_count=10,
    ):
        """Helper to create a LinkFetcher instance with supplied mocks."""
        if backend_mock is None:
            backend_mock = Mock()
        if download_manager_mock is None:
            download_manager_mock = Mock()

        return (
            LinkFetcher(
                download_manager=download_manager_mock,
                backend=backend_mock,
                statement_id="statement-123",
                initial_links=list(initial_links),
                total_chunk_count=total_chunk_count,
            ),
            backend_mock,
            download_manager_mock,
        )

    def test_add_links_and_get_next_chunk_index(self, sample_links):
        """Verify that initial links are stored and next chunk index is computed correctly."""
        link0, link1 = sample_links

        fetcher, _backend, download_manager = self._create_fetcher([link0])

        # add_link should have been called for the initial link
        download_manager.add_links.assert_called_once()

        # Internal mapping should contain the link
        assert fetcher.chunk_index_to_link[0] == link0

        # The next chunk index should be 1 (from link0.next_chunk_index)
        assert fetcher._get_next_chunk_index() == 1

        # Add second link and validate it is present
        fetcher._add_links([link1])
        assert fetcher.chunk_index_to_link[1] == link1

    def test_trigger_next_batch_download_success(self, sample_links):
        """Check that _trigger_next_batch_download fetches and stores new links."""
        link0, link1 = sample_links

        backend_mock = Mock()
        backend_mock.get_chunk_links = Mock(return_value=[link1])

        fetcher, backend, download_manager = self._create_fetcher(
            [link0], backend_mock=backend_mock
        )

        # Trigger download of the next chunk (index 1)
        success = fetcher._trigger_next_batch_download()

        assert success is True
        backend.get_chunk_links.assert_called_once_with("statement-123", 1)
        assert fetcher.chunk_index_to_link[1] == link1
        # Two calls to add_link: one for initial link, one for new link
        assert download_manager.add_links.call_count == 2

    def test_trigger_next_batch_download_error(self, sample_links):
        """Ensure that errors from backend are captured and surfaced."""
        link0, _link1 = sample_links

        backend_mock = Mock()
        backend_mock.get_chunk_links.side_effect = ServerOperationError(
            "Backend failure"
        )

        fetcher, backend, download_manager = self._create_fetcher(
            [link0], backend_mock=backend_mock
        )

        success = fetcher._trigger_next_batch_download()

        assert success is False
        assert fetcher._error is not None

    def test_get_chunk_link_waits_until_available(self, sample_links):
        """Validate that get_chunk_link blocks until the requested link is available and then returns it."""
        link0, link1 = sample_links

        backend_mock = Mock()
        # Configure backend to return link1 when requested for chunk index 1
        backend_mock.get_chunk_links = Mock(return_value=[link1])

        fetcher, backend, download_manager = self._create_fetcher(
            [link0], backend_mock=backend_mock, total_chunk_count=2
        )

        # Holder to capture the link returned from the background thread
        result_container = {}

        def _worker():
            result_container["link"] = fetcher.get_chunk_link(1)

        thread = threading.Thread(target=_worker)
        thread.start()

        # Give the thread a brief moment to start and attempt to fetch (and therefore block)
        time.sleep(0.1)

        # Trigger the backend fetch which will add link1 and notify waiting threads
        fetcher._trigger_next_batch_download()

        thread.join(timeout=2)

        # The thread should have finished and captured link1
        assert result_container.get("link") == link1

    def test_get_chunk_link_out_of_range_returns_none(self, sample_links):
        """Requesting a chunk index >= total_chunk_count should immediately return None."""
        link0, _ = sample_links

        fetcher, _backend, _dm = self._create_fetcher([link0], total_chunk_count=1)

        assert fetcher.get_chunk_link(10) is None
