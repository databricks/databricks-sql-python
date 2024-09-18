import pytest
import unittest
from unittest.mock import MagicMock, patch
from ssl import create_default_context

from databricks.sql.thrift_api.TCLIService.ttypes import TSparkArrowResultLink
import databricks.sql.utils as utils
from tests.e2e.common.predicates import pysql_supports_arrow

try:
    import pyarrow
except ImportError:
    pyarrow = None

@pytest.mark.skipif(not pysql_supports_arrow(), reason="Skipping because pyarrow is not installed")
class CloudFetchQueueSuite(unittest.TestCase):

    def create_result_link(
            self,
            file_link: str = "fileLink",
            start_row_offset: int = 0,
            row_count: int = 8000,
            bytes_num: int = 20971520
    ):
        return TSparkArrowResultLink(file_link, None, start_row_offset, row_count, bytes_num)

    def create_result_links(self, num_files: int, start_row_offset: int = 0):
        result_links = []
        for i in range(num_files):
            file_link = "fileLink_" + str(i)
            result_link = self.create_result_link(file_link=file_link, start_row_offset=start_row_offset)
            result_links.append(result_link)
            start_row_offset += result_link.rowCount
        return result_links

    @staticmethod
    def make_arrow_table():
        batch = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]
        n_cols = len(batch[0]) if batch else 0
        schema = pyarrow.schema({"col%s" % i: pyarrow.uint32() for i in range(n_cols)})
        cols = [[batch[row][col] for row in range(len(batch))] for col in range(n_cols)]
        return pyarrow.Table.from_pydict(dict(zip(schema.names, cols)), schema=schema)

    @staticmethod
    def get_schema_bytes():
        schema = pyarrow.schema({"col%s" % i: pyarrow.uint32() for i in range(4)})
        sink = pyarrow.BufferOutputStream()
        writer = pyarrow.ipc.RecordBatchStreamWriter(sink, schema)
        writer.close()
        return sink.getvalue().to_pybytes()


    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table", return_value=[None, None])
    def test_initializer_adds_links(self, mock_create_next_table):
        schema_bytes = MagicMock()
        result_links = self.create_result_links(10)
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=result_links,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )

        assert len(queue.download_manager._pending_links) == 10
        assert len(queue.download_manager._download_tasks) == 0
        mock_create_next_table.assert_called()

    def test_initializer_no_links_to_add(self):
        schema_bytes = MagicMock()
        result_links = []
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=result_links,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )

        assert len(queue.download_manager._pending_links) == 0
        assert len(queue.download_manager._download_tasks) == 0
        assert queue.table is None

    @patch("databricks.sql.cloudfetch.download_manager.ResultFileDownloadManager.get_next_downloaded_file", return_value=None)
    def test_create_next_table_no_download(self, mock_get_next_downloaded_file):
        queue = utils.CloudFetchQueue(
            MagicMock(),
            result_links=[],
            max_download_threads=10,
            ssl_context=create_default_context(),
        )

        assert queue._create_next_table() is None
        mock_get_next_downloaded_file.assert_called_with(0)

    @patch("databricks.sql.utils.create_arrow_table_from_arrow_file")
    @patch("databricks.sql.cloudfetch.download_manager.ResultFileDownloadManager.get_next_downloaded_file",
           return_value=MagicMock(file_bytes=b"1234567890", row_count=4))
    def test_initializer_create_next_table_success(self, mock_get_next_downloaded_file, mock_create_arrow_table):
        mock_create_arrow_table.return_value = self.make_arrow_table()
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        expected_result = self.make_arrow_table()

        mock_get_next_downloaded_file.assert_called_with(0)
        mock_create_arrow_table.assert_called_with(b"1234567890", description)
        assert queue.table == expected_result
        assert queue.table.num_rows == 4
        assert queue.table_row_index == 0
        assert queue.start_row_index == 4

        table = queue._create_next_table()
        assert table == expected_result
        assert table.num_rows == 4
        assert queue.start_row_index == 8

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_next_n_rows_0_rows(self, mock_create_next_table):
        mock_create_next_table.return_value = self.make_arrow_table()
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        assert queue.table_row_index == 0

        result = queue.next_n_rows(0)
        assert result.num_rows == 0
        assert queue.table_row_index == 0
        assert result == self.make_arrow_table()[0:0]

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_next_n_rows_partial_table(self, mock_create_next_table):
        mock_create_next_table.return_value = self.make_arrow_table()
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        assert queue.table_row_index == 0

        result = queue.next_n_rows(3)
        assert result.num_rows == 3
        assert queue.table_row_index == 3
        assert result == self.make_arrow_table()[:3]

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_next_n_rows_more_than_one_table(self, mock_create_next_table):
        mock_create_next_table.return_value = self.make_arrow_table()
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        assert queue.table_row_index == 0

        result = queue.next_n_rows(7)
        assert result.num_rows == 7
        assert queue.table_row_index == 3
        assert result == pyarrow.concat_tables([self.make_arrow_table(), self.make_arrow_table()])[:7]

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_next_n_rows_only_one_table_returned(self, mock_create_next_table):
        mock_create_next_table.side_effect = [self.make_arrow_table(), None]
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        assert queue.table_row_index == 0

        result = queue.next_n_rows(7)
        assert result.num_rows == 4
        assert result == self.make_arrow_table()

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table", return_value=None)
    def test_next_n_rows_empty_table(self, mock_create_next_table):
        schema_bytes = self.get_schema_bytes()
        description = MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table is None

        result = queue.next_n_rows(100)
        mock_create_next_table.assert_called()
        assert result == pyarrow.ipc.open_stream(bytearray(schema_bytes)).read_all()

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_remaining_rows_empty_table_fully_returned(self, mock_create_next_table):
        mock_create_next_table.side_effect = [self.make_arrow_table(), None, 0]
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        queue.table_row_index = 4

        result = queue.remaining_rows()
        assert result.num_rows == 0
        assert result == self.make_arrow_table()[0:0]

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_remaining_rows_partial_table_fully_returned(self, mock_create_next_table):
        mock_create_next_table.side_effect = [self.make_arrow_table(), None]
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        queue.table_row_index = 2

        result = queue.remaining_rows()
        assert result.num_rows == 2
        assert result == self.make_arrow_table()[2:]

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_remaining_rows_one_table_fully_returned(self, mock_create_next_table):
        mock_create_next_table.side_effect = [self.make_arrow_table(), None]
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        assert queue.table_row_index == 0

        result = queue.remaining_rows()
        assert result.num_rows == 4
        assert result == self.make_arrow_table()

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table")
    def test_remaining_rows_multiple_tables_fully_returned(self, mock_create_next_table):
        mock_create_next_table.side_effect = [self.make_arrow_table(), self.make_arrow_table(), None]
        schema_bytes, description = MagicMock(), MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table == self.make_arrow_table()
        assert queue.table.num_rows == 4
        queue.table_row_index = 3

        result = queue.remaining_rows()
        assert mock_create_next_table.call_count == 3
        assert result.num_rows == 5
        assert result == pyarrow.concat_tables([self.make_arrow_table(), self.make_arrow_table()])[3:]

    @patch("databricks.sql.utils.CloudFetchQueue._create_next_table", return_value=None)
    def test_remaining_rows_empty_table(self, mock_create_next_table):
        schema_bytes = self.get_schema_bytes()
        description = MagicMock()
        queue = utils.CloudFetchQueue(
            schema_bytes,
            result_links=[],
            description=description,
            max_download_threads=10,
            ssl_context=create_default_context(),
        )
        assert queue.table is None

        result = queue.remaining_rows()
        assert result == pyarrow.ipc.open_stream(bytearray(schema_bytes)).read_all()
