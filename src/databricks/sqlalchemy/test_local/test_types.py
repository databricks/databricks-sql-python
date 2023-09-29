import pytest

from databricks.sqlalchemy import DatabricksDialect
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Double,
    Enum,
    Float,
    Integer,
    Interval,
    LargeBinary,
    MatchType,
    Numeric,
    PickleType,
    SchemaType,
    SmallInteger,
    String,
    Text,
    Time,
    Unicode,
    UnicodeText,
    Uuid,
)


class TesteDatabricksTypeTests:
    def test_basic_example(self):
        assert False


class TestCamelCaseTypes:
    """Per the sqlalchemy documentation here: https://docs.sqlalchemy.org/en/20/core/type_basics.html#generic-camelcase-types

    These are the default types that are expected to work across all dialects. These tests verify that they render as expected.
    """

    def test_bigint(self):

        target = BigInteger().compile(dialect=DatabricksDialect)
        assert False
