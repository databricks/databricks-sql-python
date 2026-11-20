"""
Tests for the ResultSetFilter class.
"""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from databricks.sql.backend.sea.utils.filters import ResultSetFilter


class TestResultSetFilter(unittest.TestCase):
    """Tests for the ResultSetFilter class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock SeaResultSet
        self.mock_sea_result_set = MagicMock()

        # Set up fetchall_json, which the filters use to read all remaining rows
        self.mock_sea_result_set.results = MagicMock()
        self.mock_sea_result_set.fetchall_json.return_value = [
            ["catalog1", "schema1", "table1", "owner1", "2023-01-01", "TABLE", ""],
            ["catalog1", "schema1", "table2", "owner1", "2023-01-01", "VIEW", ""],
            [
                "catalog1",
                "schema1",
                "table3",
                "owner1",
                "2023-01-01",
                "SYSTEM TABLE",
                "",
            ],
            [
                "catalog1",
                "schema1",
                "table4",
                "owner1",
                "2023-01-01",
                "EXTERNAL TABLE",
                "",
            ],
        ]

        # Set up the connection and other required attributes
        self.mock_sea_result_set.connection = MagicMock()
        self.mock_sea_result_set.backend = MagicMock()
        self.mock_sea_result_set.buffer_size_bytes = 1000
        self.mock_sea_result_set.arraysize = 100
        self.mock_sea_result_set.statement_id = "test-statement-id"
        self.mock_sea_result_set.lz4_compressed = False

        # Create a mock CommandId
        from databricks.sql.backend.types import CommandId, BackendType

        mock_command_id = CommandId(BackendType.SEA, "test-statement-id")
        self.mock_sea_result_set.command_id = mock_command_id

        self.mock_sea_result_set.status = MagicMock()
        self.mock_sea_result_set.description = [
            ("catalog_name", "string", None, None, None, None, True),
            ("schema_name", "string", None, None, None, None, True),
            ("table_name", "string", None, None, None, None, True),
            ("owner", "string", None, None, None, None, True),
            ("creation_time", "string", None, None, None, None, True),
            ("table_type", "string", None, None, None, None, True),
            ("remarks", "string", None, None, None, None, True),
        ]
        self.mock_sea_result_set.has_been_closed_server_side = False
        self.mock_sea_result_set._arrow_schema_bytes = None

    def test__filter_json_result_set(self):
        """Test filtering by column values with various options."""
        # Case 1: Case-sensitive filtering
        allowed_values = ["table1", "table3"]

        with patch(
            "databricks.sql.backend.sea.utils.filters.isinstance", return_value=True
        ):
            with patch(
                "databricks.sql.backend.sea.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                # Call _filter_json_result_set on the table_name column (index 2)
                result = ResultSetFilter._filter_json_result_set(
                    self.mock_sea_result_set, 2, allowed_values, case_sensitive=True
                )

                # Verify the filter was applied correctly
                mock_sea_result_set_class.assert_called_once()

                # Check the filtered data passed to the constructor
                args, kwargs = mock_sea_result_set_class.call_args
                result_data = kwargs.get("result_data")
                self.assertIsNotNone(result_data)
                self.assertEqual(len(result_data.data), 2)
                self.assertIn(result_data.data[0][2], allowed_values)
                self.assertIn(result_data.data[1][2], allowed_values)

        # Case 2: Case-insensitive filtering
        mock_sea_result_set_class.reset_mock()
        with patch(
            "databricks.sql.backend.sea.utils.filters.isinstance", return_value=True
        ):
            with patch(
                "databricks.sql.backend.sea.result_set.SeaResultSet"
            ) as mock_sea_result_set_class:
                mock_instance = MagicMock()
                mock_sea_result_set_class.return_value = mock_instance

                # Call _filter_json_result_set with case-insensitive matching
                result = ResultSetFilter._filter_json_result_set(
                    self.mock_sea_result_set,
                    2,
                    ["TABLE1", "TABLE3"],
                    case_sensitive=False,
                )
                mock_sea_result_set_class.assert_called_once()

    def test_filter_tables_by_type(self):
        """Test filtering tables by type with various options."""
        # Case 1: Specific table types
        table_types = ["TABLE", "VIEW"]

        # Mock results as JsonQueue (not CloudFetchQueue or ArrowQueue)
        from databricks.sql.backend.sea.queue import JsonQueue

        self.mock_sea_result_set.results = JsonQueue([])

        with patch.object(ResultSetFilter, "_filter_json_result_set") as mock_filter:
            ResultSetFilter.filter_tables_by_type(self.mock_sea_result_set, table_types)
            args, kwargs = mock_filter.call_args
            self.assertEqual(args[0], self.mock_sea_result_set)
            self.assertEqual(kwargs.get("column_index"), 5)  # Table type column index
            self.assertEqual(kwargs.get("allowed_values"), table_types)
            self.assertEqual(kwargs.get("case_sensitive"), True)

        # Case 2: Default table types (None or empty list)
        with patch.object(ResultSetFilter, "_filter_json_result_set") as mock_filter:
            # Test with None
            ResultSetFilter.filter_tables_by_type(self.mock_sea_result_set, None)
            args, kwargs = mock_filter.call_args
            self.assertEqual(
                kwargs.get("allowed_values"), ["TABLE", "VIEW", "SYSTEM TABLE"]
            )

            # Test with empty list
            ResultSetFilter.filter_tables_by_type(self.mock_sea_result_set, [])
            args, kwargs = mock_filter.call_args
            self.assertEqual(
                kwargs.get("allowed_values"), ["TABLE", "VIEW", "SYSTEM TABLE"]
            )


class TestResultSetFilterWithFetchoneBuffer(unittest.TestCase):
    """Rows buffered by fetchone() on a real SeaResultSet must reach the filter."""

    N_ROWS = 12

    def _row_type(self, i):
        return "TABLE" if i % 2 == 0 else "VIEW"

    def _make_result_set(self, queue, result_format):
        from databricks.sql.backend.sea.models.base import ResultData, ResultManifest
        from databricks.sql.backend.sea.result_set import SeaResultSet
        from databricks.sql.backend.types import CommandId, CommandState

        connection = MagicMock()
        connection.open = True
        connection.disable_pandas = True
        sea_client = MagicMock()
        sea_client.max_download_threads = 10

        execute_response = MagicMock()
        execute_response.command_id = CommandId.from_sea_statement_id("stmt-1")
        execute_response.status = CommandState.SUCCEEDED
        execute_response.has_been_closed_server_side = False
        execute_response.description = [
            ("table_name", "string", None, None, None, None, None),
            ("table_type", "string", None, None, None, None, None),
        ]
        execute_response.is_staging_operation = False
        execute_response.lz4_compressed = False
        execute_response.arrow_schema_bytes = None

        manifest = ResultManifest(
            format=result_format.value,
            schema={},
            total_row_count=self.N_ROWS,
            total_byte_count=-1,
            total_chunk_count=-1,
        )
        with patch(
            "databricks.sql.backend.sea.queue.SeaResultSetQueueFactory.build_queue",
            return_value=queue,
        ):
            return SeaResultSet(
                connection=connection,
                execute_response=execute_response,
                sea_client=sea_client,
                result_data=ResultData(data=None, external_links=None),
                manifest=manifest,
                buffer_size_bytes=1000,
                arraysize=100,
            )

    def _consume_two_rows(self, rs):
        # 1st fetchone() refills 1 row, 2nd refills 10: rows 2..10 stay buffered
        # and row 11 is still in the queue.
        self.assertEqual(rs.fetchone()[0], "t0")
        self.assertEqual(rs.fetchone()[0], "t1")
        self.assertGreater(rs._row_buffer_count(), 0)

    def _expected_names(self):
        return [
            "t{}".format(i)
            for i in range(2, self.N_ROWS)
            if self._row_type(i) == "TABLE"
        ]

    def test_json_filter_includes_rows_buffered_by_fetchone(self):
        from databricks.sql.backend.sea.queue import JsonQueue
        from databricks.sql.backend.sea.utils.constants import ResultFormat

        data = [["t{}".format(i), self._row_type(i)] for i in range(self.N_ROWS)]
        rs = self._make_result_set(JsonQueue(data), ResultFormat.JSON_ARRAY)
        self._consume_two_rows(rs)

        with patch(
            "databricks.sql.backend.sea.result_set.SeaResultSet"
        ) as mock_sea_result_set_class:
            ResultSetFilter._filter_json_result_set(
                rs, 1, ["TABLE"], case_sensitive=True
            )
            result_data = mock_sea_result_set_class.call_args.kwargs["result_data"]

        self.assertEqual([r[0] for r in result_data.data], self._expected_names())
        self.assertEqual(rs.rownumber, self.N_ROWS)

    def test_arrow_filter_includes_rows_buffered_by_fetchone(self):
        pyarrow = pytest.importorskip("pyarrow")
        from databricks.sql.backend.sea.utils.constants import ResultFormat
        from databricks.sql.utils import ArrowQueue

        table = pyarrow.Table.from_pydict(
            {
                "table_name": ["t{}".format(i) for i in range(self.N_ROWS)],
                "table_type": [self._row_type(i) for i in range(self.N_ROWS)],
            }
        )
        rs = self._make_result_set(
            ArrowQueue(table, self.N_ROWS), ResultFormat.ARROW_STREAM
        )
        self._consume_two_rows(rs)

        with patch(
            "databricks.sql.backend.sea.result_set.SeaResultSet"
        ) as mock_sea_result_set_class:
            ResultSetFilter._filter_arrow_result_set(
                rs, 1, ["TABLE"], case_sensitive=True
            )
            result_data = mock_sea_result_set_class.call_args.kwargs["result_data"]

        filtered = pyarrow.ipc.open_stream(result_data.attachment).read_all()
        self.assertEqual(
            filtered.column("table_name").to_pylist(), self._expected_names()
        )
        self.assertEqual(rs.rownumber, self.N_ROWS)


if __name__ == "__main__":
    unittest.main()
