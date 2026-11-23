"""Tests for _create_rows and the result conversions that build rows with it."""

import pickle
from decimal import Decimal
from types import SimpleNamespace

import numpy
import pytest

try:
    import pyarrow as pa
except ImportError:
    pa = None

from databricks.sql.backend.sea.result_set import SeaResultSet
from databricks.sql.result_set import ResultSet, ThriftResultSet
from databricks.sql.types import Row, _create_rows
from databricks.sql.utils import ColumnTable

FIELDS = Row("id", "name", "amount")

ROW_INPUTS = {
    "tuples": lambda: [(1, "a", Decimal("1.10")), (2, None, None)],
    "lists": lambda: [[1, "a", Decimal("1.10")], [2, None, None]],
    "numpy_object_rows": lambda: numpy.array(
        [[1, "a", Decimal("1.10")], [2, None, 3.5]], dtype="object"
    ),
    "generator": lambda: ([i, str(i), None] for i in range(3)),
    "fewer_values_than_fields": lambda: [(1,), (2, "b")],
    "no_rows": lambda: [],
}


def _outcome(function):
    try:
        return ("value", function())
    except Exception as error:
        return ("error", type(error))


def _assert_same_rows(expected, actual):
    assert len(actual) == len(expected)
    for want, got in zip(expected, actual):
        assert type(got) is Row
        assert tuple(got) == tuple(want)
        assert [type(value) for value in got] == [type(value) for value in want]
        assert got.__fields__ is want.__fields__
        assert repr(got) == repr(want)
        assert got.asDict() == want.asDict()
        assert got == want
        for name in want.__fields__:
            assert _outcome(lambda: getattr(got, name)) == _outcome(
                lambda: getattr(want, name)
            )
            assert _outcome(lambda: got[name]) == _outcome(lambda: want[name])
            assert (name in got) == (name in want)


@pytest.mark.parametrize("make_rows", ROW_INPUTS.values(), ids=ROW_INPUTS.keys())
def test_create_rows_matches_row_factory(make_rows):
    _assert_same_rows(
        [FIELDS(*values) for values in make_rows()], _create_rows(FIELDS, make_rows())
    )


def test_create_rows_pickles_like_row_factory():
    want = FIELDS(1, "a", None)
    (got,) = _create_rows(FIELDS, [(1, "a", None)])
    assert pickle.dumps(got) == pickle.dumps(want)
    restored = pickle.loads(pickle.dumps(got))
    assert restored == want
    assert restored.asDict() == want.asDict()


def test_create_rows_rejects_more_values_than_fields_like_row_factory():
    with pytest.raises(ValueError) as expected:
        FIELDS(1, "a", None, "extra")
    with pytest.raises(ValueError) as actual:
        _create_rows(FIELDS, [(1, "a", None), (1, "a", None, "extra")])
    assert str(actual.value) == str(expected.value)


def test_create_rows_keeps_rows_read_only():
    (row,) = _create_rows(FIELDS, [(1, "a", None)])
    with pytest.raises(RuntimeError):
        row.id = 2


DESCRIPTION = [
    ("id", "bigint", None, None, None, None, None),
    ("name", "string", None, None, None, None, None),
]
EXPECTED_VALUES = [(1, "a"), (None, None), (3, "c")]


def _assert_rows_for_description(rows):
    expected = [Row("id", "name")(*values) for values in EXPECTED_VALUES]
    assert rows == expected
    assert all(type(row) is Row for row in rows)
    assert {id(row.__fields__) for row in rows} == {id(rows[0].__fields__)}
    assert rows[0].__fields__ == Row("id", "name")
    assert [row.name for row in rows] == ["a", None, "c"]


@pytest.mark.skipif(pa is None, reason="PyArrow is not installed")
@pytest.mark.parametrize("disable_pandas", [False, True])
def test_convert_arrow_table_builds_rows(disable_pandas):
    table = pa.table(
        {
            "id": pa.array([1, None, 3], pa.int64()),
            "name": pa.array(["a", None, "c"], pa.string()),
        }
    )
    result_set = SimpleNamespace(
        description=DESCRIPTION,
        connection=SimpleNamespace(disable_pandas=disable_pandas),
    )
    _assert_rows_for_description(ResultSet._convert_arrow_table(result_set, table))


def test_convert_columnar_table_builds_rows():
    table = ColumnTable([[1, None, 3], ["a", None, "c"]], ["id", "name"])
    result_set = SimpleNamespace(description=DESCRIPTION)
    _assert_rows_for_description(
        ThriftResultSet._convert_columnar_table(result_set, table)
    )


def test_create_json_table_builds_rows():
    result_set = SimpleNamespace(
        description=DESCRIPTION, _convert_json_types=lambda row: list(row)
    )
    _assert_rows_for_description(
        SeaResultSet._create_json_table(result_set, [list(v) for v in EXPECTED_VALUES])
    )
