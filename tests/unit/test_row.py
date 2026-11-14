import pickle

import pytest

from databricks.sql.types import Row, _create_row


def test_row_factory_builds_named_rows():
    Person = Row("name", "age")
    row = Person("Alice", 11)
    assert type(row) is Row
    assert row == ("Alice", 11)
    assert row.__fields__ is Person
    assert row.__dict__ == {"__fields__": Person}
    assert row.name == "Alice" and row["age"] == 11 and row[0] == "Alice"
    assert row.asDict() == {"name": "Alice", "age": 11}
    assert repr(row) == "Row(name='Alice', age=11)"
    assert "name" in row and "wrong" not in row
    assert row == Row(name="Alice", age=11)


def test_row_factory_accepts_fewer_values_and_rejects_more():
    Person = Row("name", "age")
    assert Person("Alice") == ("Alice",)
    assert Person() == ()
    with pytest.raises(ValueError, match="expected 2 values but got"):
        Person("Alice", 11, "extra")


def test_rows_stay_read_only():
    row = Row("a")(1)
    with pytest.raises(RuntimeError, match="read-only"):
        row.a = 2


def test_create_row_matches_the_factory():
    fields = ["a", "b"]
    row = _create_row(fields, [1, 2])
    assert type(row) is Row
    assert row == (1, 2)
    assert row.__fields__ is fields
    assert row.asDict() == {"a": 1, "b": 2}


def test_pickle_round_trip():
    row = Row("a", "b")(1, None)
    copy = pickle.loads(pickle.dumps(row))
    assert copy == row and type(copy) is Row
    assert copy.asDict() == {"a": 1, "b": None}
    assert repr(copy) == repr(row)
