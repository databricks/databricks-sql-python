import decimal
import datetime
import gc
import mmap
import sys
import weakref
from datetime import timezone, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

import lz4.frame
import pytest

import databricks.sql.utils as utils
from databricks.sql.utils import (
    convert_arrow_based_set_to_arrow_table,
    convert_to_assigned_datatypes_in_column_table,
    ColumnTable,
    concat_table_chunks,
    serialize_query_tags,
)

try:
    import pyarrow
except ImportError:
    pyarrow = None


class TestUtils:
    def get_column_table_and_description(self):
        table_description = [
            ("id", "int", None, None, None, None, None),
            ("varchar_column", "string", None, None, None, None, None),
            ("boolean_column", "boolean", None, None, None, None, None),
            ("integer_column", "int", None, None, None, None, None),
            ("bigint_column", "bigint", None, None, None, None, None),
            ("smallint_column", "smallint", None, None, None, None, None),
            ("tinyint_column", "tinyint", None, None, None, None, None),
            ("float_column", "float", None, None, None, None, None),
            ("double_column", "double", None, None, None, None, None),
            ("decimal_column", "decimal", None, None, 10, 2, None),
            ("date_column", "date", None, None, None, None, None),
            ("timestamp_column", "timestamp", None, None, None, None, None),
            ("timestamp_ntz_column", "timestamp", None, None, None, None, None),
            ("timestamp_column_2", "timestamp", None, None, None, None, None),
            ("timestamp_column_3", "timestamp", None, None, None, None, None),
            ("timestamp_column_4", "timestamp", None, None, None, None, None),
            ("timestamp_column_5", "timestamp", None, None, None, None, None),
            ("timestamp_column_6", "timestamp", None, None, None, None, None),
            ("timestamp_column_7", "timestamp", None, None, None, None, None),
            ("binary_column", "binary", None, None, None, None, None),
            ("array_column", "array", None, None, None, None, None),
            ("map_column", "map", None, None, None, None, None),
            ("struct_column", "struct", None, None, None, None, None),
            ("variant_column", "string", None, None, None, None, None),
        ]

        column_table = [
            (9,),
            ("Test Varchar",),
            (True,),
            (123,),
            (9876543210,),
            (32000,),
            (120,),
            (1.23,),
            (4.56,),
            ("7890.12",),
            ("2023-12-31",),
            ("2023-12-31 12:30:00",),
            ("2023-12-31 12:30:00",),
            ("2021-09-30 11:27:35.123",),
            ("03/08/2024 02:30:15 PM",),
            ("08-Mar-2024 14:30:15",),
            ("2024-03-16T14:30:25.123",),
            ("2025-03-16T12:30:45+0530",),
            ("2025-03-16 12:30:45 +0530",),
            (b"\xde\xad\xbe\xef",),
            ('["item1","item2"]',),
            ('{"key1":"value1","key2":"value2"}',),
            ('{"name":"John","age":30}',),
            ('"semi-structured data"',),
        ]

        return column_table, table_description

    def test_convert_to_assigned_datatypes_in_column_table(self):
        column_table, description = self.get_column_table_and_description()
        converted_column_table = convert_to_assigned_datatypes_in_column_table(
            column_table, description
        )

        # (data , datatype)
        expected_convertion = [
            (9, int),
            ("Test Varchar", str),
            (True, bool),
            (123, int),
            (9876543210, int),
            (32000, int),
            (120, int),
            (1.23, float),
            (4.56, float),
            (decimal.Decimal("7890.12"), decimal.Decimal),
            (datetime.date(2023, 12, 31), datetime.date),
            (datetime.datetime(2023, 12, 31, 12, 30, 0), datetime.datetime),
            (datetime.datetime(2023, 12, 31, 12, 30, 0), datetime.datetime),
            (datetime.datetime(2021, 9, 30, 11, 27, 35, 123000), datetime.datetime),
            (datetime.datetime(2024, 3, 8, 14, 30, 15), datetime.datetime),
            (datetime.datetime(2024, 3, 8, 14, 30, 15), datetime.datetime),
            (datetime.datetime(2024, 3, 16, 14, 30, 25, 123000), datetime.datetime),
            (
                datetime.datetime(
                    2025,
                    3,
                    16,
                    12,
                    30,
                    45,
                    tzinfo=timezone(timedelta(hours=5, minutes=30)),
                ),
                datetime.datetime,
            ),
            (
                datetime.datetime(
                    2025,
                    3,
                    16,
                    12,
                    30,
                    45,
                    tzinfo=timezone(timedelta(hours=5, minutes=30)),
                ),
                datetime.datetime,
            ),
            (b"\xde\xad\xbe\xef", bytes),
            ('["item1","item2"]', str),
            ('{"key1":"value1","key2":"value2"}', str),
            ('{"name":"John","age":30}', str),
            ('"semi-structured data"', str),
        ]

        for index, entry in enumerate(converted_column_table):
            assert entry[0] == expected_convertion[index][0]
            assert isinstance(entry[0], expected_convertion[index][1])

    def test_concat_table_chunks_column_table(self):
        column_table1 = ColumnTable([[1, 2], [5, 6]], ["col1", "col2"])
        column_table2 = ColumnTable([[3, 4], [7, 8]], ["col1", "col2"])

        result_table = concat_table_chunks([column_table1, column_table2])

        assert result_table.column_table == [[1, 2, 3, 4], [5, 6, 7, 8]]
        assert result_table.column_names == ["col1", "col2"]

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_concat_table_chunks_arrow_table(self):
        arrow_table1 = pyarrow.Table.from_pydict({"col1": [1, 2], "col2": [5, 6]})
        arrow_table2 = pyarrow.Table.from_pydict({"col1": [3, 4], "col2": [7, 8]})

        result_table = concat_table_chunks([arrow_table1, arrow_table2])
        assert result_table.column_names == ["col1", "col2"]
        assert result_table.column("col1").to_pylist() == [1, 2, 3, 4]
        assert result_table.column("col2").to_pylist() == [5, 6, 7, 8]

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_concat_table_chunks_arrow_old_pyarrow_fallback(self, monkeypatch):
        # pyarrow < 14 has no `promote_options` kwarg; a base connector install
        # can run against a runtime's old bundled pyarrow (e.g. DBR 13.3/14.3).
        # concat_table_chunks must fall back to the legacy `promote=True` API
        # rather than raise TypeError (regression: DBR LTS install check).
        arrow_table1 = pyarrow.Table.from_pydict({"col1": [1, 2]})
        arrow_table2 = pyarrow.Table.from_pydict({"col1": [3, 4]})

        real_concat = pyarrow.concat_tables

        def old_concat(tables, promote_options=None, **kwargs):
            if promote_options is not None:
                raise TypeError(
                    "concat_tables() got an unexpected keyword argument 'promote_options'"
                )
            return real_concat(tables, **kwargs)

        monkeypatch.setattr(pyarrow, "concat_tables", old_concat)
        result_table = concat_table_chunks([arrow_table1, arrow_table2])
        assert result_table.column("col1").to_pylist() == [1, 2, 3, 4]

    @pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
    def test_concat_table_chunks_duplicate_column_names(self):
        # Duplicate column names are valid SQL (e.g. SELECT 1 AS a, 2 AS a).
        # Schema promotion rejects them with ArrowInvalid, so when chunk
        # schemas match we must concatenate without promotion (GH #989).
        arrow_table1 = pyarrow.Table.from_arrays(
            [pyarrow.array([1]), pyarrow.array([2])], names=["a", "a"]
        )
        arrow_table2 = pyarrow.Table.from_arrays(
            [pyarrow.array([3]), pyarrow.array([4])], names=["a", "a"]
        )

        result_table = concat_table_chunks([arrow_table1, arrow_table2])
        assert result_table.column_names == ["a", "a"]
        assert result_table.column(0).to_pylist() == [1, 3]
        assert result_table.column(1).to_pylist() == [2, 4]

    def test_concat_table_chunks_empty(self):
        result_table = concat_table_chunks([])
        assert result_table == []

    def test_concat_table_chunks__incorrect_column_names_error(self):
        column_table1 = ColumnTable([[1, 2], [5, 6]], ["col1", "col2"])
        column_table2 = ColumnTable([[3, 4], [7, 8]], ["col1", "col3"])

        with pytest.raises(ValueError):
            concat_table_chunks([column_table1, column_table2])

    def test_serialize_query_tags_basic(self):
        """Test basic query tags serialization"""
        query_tags = {"team": "data-eng", "application": "etl"}
        result = serialize_query_tags(query_tags)
        assert result == "team:data-eng,application:etl"

    def test_serialize_query_tags_with_none_value(self):
        """Test query tags with None value (should omit colon and value)"""
        query_tags = {"key1": "value1", "key2": None, "key3": "value3"}
        result = serialize_query_tags(query_tags)
        assert result == "key1:value1,key2,key3:value3"

    def test_serialize_query_tags_with_special_chars(self):
        """Test query tags with special characters (colon, comma, backslash)"""
        query_tags = {
            "key1": "value:with:colons",
            "key2": "value,with,commas",
            "key3": r"value\with\backslashes",
        }
        result = serialize_query_tags(query_tags)
        assert (
            result
            == r"key1:value\:with\:colons,key2:value\,with\,commas,key3:value\\with\\backslashes"
        )

    def test_serialize_query_tags_with_mixed_special_chars(self):
        """Test query tags with mixed special characters"""
        query_tags = {"key1": r"a:b,c\d"}
        result = serialize_query_tags(query_tags)
        assert result == r"key1:a\:b\,c\\d"

    def test_serialize_query_tags_empty_dict(self):
        """Test serialization with empty dictionary"""
        query_tags = {}
        result = serialize_query_tags(query_tags)
        assert result is None

    def test_serialize_query_tags_none(self):
        """Test serialization with None input"""
        result = serialize_query_tags(None)
        assert result is None

    def test_serialize_query_tags_with_special_chars_in_key(self):
        """Test query tags with special characters in keys (only backslashes are escaped in keys)"""
        query_tags = {
            "key:with:colons": "value1",
            "key,with,commas": "value2",
            r"key\with\backslashes": "value3",
        }
        result = serialize_query_tags(query_tags)
        # Only backslashes are escaped in keys; colons and commas in keys are not escaped
        assert (
            result
            == r"key:with:colons:value1,key,with,commas:value2,key\\with\\backslashes:value3"
        )

    def test_serialize_query_tags_all_none_values(self):
        """Test query tags where all values are None"""
        query_tags = {"key1": None, "key2": None, "key3": None}
        result = serialize_query_tags(query_tags)
        assert result == "key1,key2,key3"


def _inline_arrow_set(num_batches, rows_per_batch, lz4_compressed, constant=False):
    n = num_batches * rows_per_batch
    table = pyarrow.table(
        {
            "id": [0] * n if constant else list(range(n)),
            "s": ["x" * 20] * n if constant else [f"row-{i}" for i in range(n)],
        }
    )
    schema_bytes = table.schema.serialize().to_pybytes()
    batches = []
    for batch in table.to_batches(max_chunksize=rows_per_batch):
        data = batch.serialize().to_pybytes()
        batches.append(
            SimpleNamespace(
                batch=lz4.frame.compress(data) if lz4_compressed else data,
                rowCount=batch.num_rows,
            )
        )
    return table, schema_bytes, batches


def _stream_size(schema_bytes, batches):
    return len(schema_bytes) + sum(len(lz4.frame.decompress(b.batch)) for b in batches)


def _large_inline_arrow_set(lz4_compressed=True):
    table, schema_bytes, batches = _inline_arrow_set(4, 50000, lz4_compressed)
    # The map-path tests need streams above the mmap floor even with a 1x estimate
    assert sum(len(b.batch) for b in batches) >= utils._INLINE_ARROW_MMAP_MIN_BYTES
    return table, schema_bytes, batches


def _patch_map(map_factory):
    """Replace the mmap module seen by utils only, not the stdlib module itself."""
    fake = SimpleNamespace(
        mmap=map_factory, MAP_PRIVATE=getattr(mmap, "MAP_PRIVATE", 0)
    )
    return patch.object(utils, "mmap", fake)


linux_only = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="inline Arrow maps are Linux-only"
)
needs_arrow_lz4 = pytest.mark.skipif(
    not utils._PYARROW_HAS_LZ4, reason="the one-call decode needs pyarrow's lz4 codec"
)


class _RecordingMap(mmap.mmap):
    resizes = []

    def resize(self, newsize):
        type(self).resizes.append((len(self), newsize))
        return super().resize(newsize)


@pytest.mark.skipif(pyarrow is None, reason="PyArrow is not installed")
class TestConvertArrowBasedSet:
    @linux_only
    @pytest.mark.parametrize("lz4_compressed", [True, False])
    def test_large_set_is_read_from_a_private_memory_map(self, lz4_compressed):
        table, schema_bytes, batches = _large_inline_arrow_set(lz4_compressed)

        mmap_spy = Mock(wraps=mmap.mmap)
        with _patch_map(mmap_spy):
            result, n_rows = convert_arrow_based_set_to_arrow_table(
                batches, lz4_compressed, schema_bytes
            )

        assert n_rows == table.num_rows
        assert result.equals(table)
        mmap_spy.assert_called_once()
        size = mmap_spy.call_args.args[1]
        assert size >= utils._INLINE_ARROW_MMAP_MIN_BYTES
        if hasattr(mmap, "MAP_PRIVATE"):
            flags = mmap_spy.call_args.kwargs["flags"]
            assert flags & mmap.MAP_PRIVATE
            assert not flags & mmap.MAP_SHARED

    @linux_only
    def test_map_grows_when_the_size_estimate_is_too_small(self):
        table, schema_bytes, batches = _large_inline_arrow_set()
        _RecordingMap.resizes = []

        with patch.object(utils, "_LZ4_RATIO_ESTIMATE", 1), _patch_map(_RecordingMap):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        assert any(new > old for old, new in _RecordingMap.resizes)
        assert _RecordingMap.resizes[-1][1] == _stream_size(schema_bytes, batches)

    @linux_only
    def test_map_is_shrunk_to_the_stream_size(self):
        table, schema_bytes, batches = _large_inline_arrow_set()
        _RecordingMap.resizes = []

        with _patch_map(_RecordingMap):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        assert len(_RecordingMap.resizes) == 1
        old, new = _RecordingMap.resizes[0]
        assert new == _stream_size(schema_bytes, batches) < old

    @linux_only
    def test_unshrinkable_map_is_sliced_to_the_stream(self):
        class NoShrink(mmap.mmap):
            def resize(self, newsize):
                if newsize < len(self):
                    raise BufferError("mmap can't resize with extant buffers exported.")
                return super().resize(newsize)

        table, schema_bytes, batches = _large_inline_arrow_set()
        buffers = []
        getbuffer = utils._ArrowStreamBuffer.getbuffer

        def recording_getbuffer(stream):
            buffer = getbuffer(stream)
            buffers.append((len(stream._map), len(buffer)))
            return buffer

        with _patch_map(NoShrink), patch.object(
            utils._ArrowStreamBuffer, "getbuffer", recording_getbuffer
        ):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        map_size, buffer_size = buffers[0]
        assert map_size > buffer_size == _stream_size(schema_bytes, batches)
        assert result.equals(table)

    @linux_only
    @needs_arrow_lz4
    def test_exactly_filled_map_is_not_resized(self):
        table, schema_bytes, batches = _large_inline_arrow_set()
        stream_size = _stream_size(schema_bytes, batches)
        _RecordingMap.resizes = []

        with _patch_map(_RecordingMap):
            buffer = utils._ArrowStreamBuffer(stream_size, stream_size)
            buffer.append(schema_bytes)
            utils._decompress_lz4_frames_into(buffer, [b.batch for b in batches])
            result = pyarrow.ipc.open_stream(buffer.getbuffer()).read_all()

        assert buffer.mapped
        assert _RecordingMap.resizes == []
        assert result.equals(table)

    @linux_only
    @pytest.mark.parametrize(
        "error",
        [
            SystemError("mmap: resizing not available--no mremap()"),
            BufferError("mmap can't resize with extant buffers exported."),
        ],
    )
    def test_falls_back_to_the_heap_when_the_map_cannot_grow(self, error):
        attempts = []

        class NoResize(mmap.mmap):
            def resize(self, newsize):
                attempts.append((len(self), newsize))
                raise error

        table, schema_bytes, batches = _large_inline_arrow_set()
        states = []
        getbuffer = utils._ArrowStreamBuffer.getbuffer

        def recording_getbuffer(stream):
            states.append((stream._map, len(stream._bytes)))
            return getbuffer(stream)

        with patch.object(utils, "_LZ4_RATIO_ESTIMATE", 1), _patch_map(
            NoResize
        ), patch.object(utils._ArrowStreamBuffer, "getbuffer", recording_getbuffer):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        assert attempts and attempts[0][1] > attempts[0][0]
        assert states == [(None, _stream_size(schema_bytes, batches))]

    @linux_only
    def test_non_posix_platform_stays_on_the_heap(self):
        table, schema_bytes, batches = _large_inline_arrow_set()
        no_map_private = SimpleNamespace(mmap=Mock(side_effect=AssertionError))

        with patch.object(utils, "mmap", no_map_private):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        no_map_private.mmap.assert_not_called()
        assert result.equals(table)

    def test_non_linux_platform_stays_on_the_heap(self):
        table, schema_bytes, batches = _large_inline_arrow_set()

        with patch.object(utils, "_INLINE_ARROW_USE_MMAP", False), _patch_map(
            Mock()
        ) as patched:
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        patched.mmap.assert_not_called()
        assert result.equals(table)

    @linux_only
    def test_map_grows_to_the_exact_size_when_doubling_fails(self):
        sizes = []

        class NoDoubling(mmap.mmap):
            def resize(self, newsize):
                sizes.append(newsize)
                if len(sizes) == 1:
                    raise OSError("ENOMEM")
                return super().resize(newsize)

        table, schema_bytes, batches = _large_inline_arrow_set()
        states = []
        getbuffer = utils._ArrowStreamBuffer.getbuffer

        def recording_getbuffer(stream):
            states.append(stream._map is not None)
            return getbuffer(stream)

        with patch.object(utils, "_LZ4_RATIO_ESTIMATE", 1), _patch_map(
            NoDoubling
        ), patch.object(utils._ArrowStreamBuffer, "getbuffer", recording_getbuffer):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        assert sizes[1] < sizes[0]
        assert states == [True]

    @linux_only
    @needs_arrow_lz4
    def test_many_lz4_batches_decode_as_one_stream_into_the_map(self):
        table, schema_bytes, batches = _inline_arrow_set(72, 3000, True)
        assert sum(len(b.batch) for b in batches) >= utils._INLINE_ARROW_MMAP_MIN_BYTES
        mmap_spy = Mock(wraps=mmap.mmap)

        with _patch_map(mmap_spy), patch.object(
            utils.lz4.frame, "decompress", wraps=lz4.frame.decompress
        ) as decompress_spy:
            result, n_rows = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert n_rows == table.num_rows
        assert result.equals(table)
        mmap_spy.assert_called_once()
        decompress_spy.assert_not_called()

    @linux_only
    def test_two_large_lz4_batches_use_the_per_batch_path_into_the_map(self):
        table, schema_bytes, batches = _inline_arrow_set(2, 100000, True)
        assert sum(len(b.batch) for b in batches) >= utils._INLINE_ARROW_MMAP_MIN_BYTES
        mmap_spy = Mock(wraps=mmap.mmap)

        with _patch_map(mmap_spy), patch.object(
            utils.lz4.frame, "decompress", wraps=lz4.frame.decompress
        ) as decompress_spy:
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        mmap_spy.assert_called_once()
        assert decompress_spy.call_count == 2

    @pytest.mark.parametrize(
        "rows_per_batch, expect_one_call",
        [(3000, False), pytest.param(100, True, marks=needs_arrow_lz4)],
    )
    def test_without_the_map_only_small_results_use_the_one_call_decode(
        self, rows_per_batch, expect_one_call
    ):
        table, schema_bytes, batches = _inline_arrow_set(72, rows_per_batch, True)
        large = sum(len(b.batch) for b in batches) >= utils._INLINE_ARROW_MMAP_MIN_BYTES
        assert large != expect_one_call

        with patch.object(utils, "_INLINE_ARROW_USE_MMAP", False), patch.object(
            utils.lz4.frame, "decompress", wraps=lz4.frame.decompress
        ) as decompress_spy:
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        assert decompress_spy.call_count == (0 if expect_one_call else 72)

    @linux_only
    def test_table_keeps_the_map_alive_and_frees_it_with_the_table(self):
        maps = []

        class TrackedMap(mmap.mmap):
            def __init__(self, *args, **kwargs):
                maps.append(weakref.ref(self))

        table, schema_bytes, batches = _large_inline_arrow_set()

        with _patch_map(TrackedMap):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )
        gc.collect()

        assert len(maps) == 1 and maps[0]() is not None
        assert result.equals(table)
        del result
        gc.collect()
        assert maps[0]() is None

    @linux_only
    def test_stream_under_the_floor_stays_on_the_heap(self):
        table, schema_bytes, batches = _inline_arrow_set(4, 10000, True)
        compressed = sum(len(b.batch) for b in batches)
        assert (
            _stream_size(schema_bytes, batches)
            < utils._INLINE_ARROW_MMAP_MIN_BYTES
            <= compressed * utils._LZ4_RATIO_ESTIMATE
        )

        with _patch_map(Mock()) as patched:
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        patched.mmap.assert_not_called()
        assert result.equals(table)

    @linux_only
    def test_compressible_stream_moves_to_a_map_past_the_floor(self):
        table, schema_bytes, batches = _inline_arrow_set(4, 50000, True, constant=True)
        compressed = sum(len(b.batch) for b in batches)
        stream_size = _stream_size(schema_bytes, batches)
        assert compressed < utils._INLINE_ARROW_MMAP_MIN_BYTES < stream_size
        _RecordingMap.resizes = []
        states = []
        getbuffer = utils._ArrowStreamBuffer.getbuffer

        def recording_getbuffer(stream):
            states.append((stream._map is not None, len(stream._bytes)))
            return getbuffer(stream)

        with _patch_map(_RecordingMap), patch.object(
            utils._ArrowStreamBuffer, "getbuffer", recording_getbuffer
        ):
            result, _ = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        assert result.equals(table)
        assert states == [(True, 0)]
        assert _RecordingMap.resizes[-1][1] == stream_size

    def test_small_set_stays_on_the_heap(self):
        table, schema_bytes, batches = _inline_arrow_set(2, 10, True)

        with _patch_map(Mock()) as patched:
            result, n_rows = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        patched.mmap.assert_not_called()
        assert n_rows == 20
        assert result.equals(table)

    @linux_only
    def test_mmap_failure_falls_back_to_bytes(self):
        table, schema_bytes, batches = _large_inline_arrow_set()

        with _patch_map(Mock(side_effect=OSError("ENOMEM"))) as patched, patch.object(
            utils.lz4.frame, "decompress", wraps=lz4.frame.decompress
        ) as decompress_spy:
            result, n_rows = convert_arrow_based_set_to_arrow_table(
                batches, True, schema_bytes
            )

        patched.mmap.assert_called_once()
        # Without a map, a large result keeps the per-batch decode, as on main
        assert decompress_spy.call_count == len(batches)
        assert n_rows == table.num_rows
        assert result.equals(table)

    def test_schema_only_set(self):
        table, schema_bytes, _ = _inline_arrow_set(1, 1, True)

        result, n_rows = convert_arrow_based_set_to_arrow_table([], True, schema_bytes)

        assert n_rows == 0
        assert result.num_rows == 0
        assert result.schema.equals(table.schema)
