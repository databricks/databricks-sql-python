import enum

import pytest
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
    TypeEngine,
    Unicode,
    UnicodeText,
    Uuid,
)

from databricks.sqlalchemy import DatabricksDialect


class DatabricksDataType(enum.Enum):
    """https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html"""

    BIGINT = enum.auto()
    BINARY = enum.auto()
    BOOLEAN = enum.auto()
    DATE = enum.auto()
    DECIMAL = enum.auto()
    DOUBLE = enum.auto()
    FLOAT = enum.auto()
    INT = enum.auto()
    INTERVAL = enum.auto()
    VOID = enum.auto()
    SMALLINT = enum.auto()
    STRING = enum.auto()
    TIMESTAMP = enum.auto()
    TIMESTAMP_NTZ = enum.auto()
    TINYINT = enum.auto()
    ARRAY = enum.auto()
    MAP = enum.auto()
    STRUCT = enum.auto()


# Defines the way that SQLAlchemy CamelCase types are compiled into Databricks SQL types.
# Note: I wish I could define this within the TestCamelCaseTypesCompilation class, but pytest doesn't like that.
camel_case_type_map = {
    BigInteger: DatabricksDataType.BIGINT,
    LargeBinary: DatabricksDataType.BINARY,
    Boolean: DatabricksDataType.BOOLEAN,
    Date: DatabricksDataType.DATE,
    DateTime: DatabricksDataType.TIMESTAMP,
    Double: DatabricksDataType.DOUBLE,
    Enum: DatabricksDataType.STRING,
    Float: DatabricksDataType.FLOAT,
    Integer: DatabricksDataType.INT,
    Interval: DatabricksDataType.TIMESTAMP,
    Numeric: DatabricksDataType.DECIMAL,
    PickleType: DatabricksDataType.BINARY,
    SmallInteger: DatabricksDataType.SMALLINT,
    String: DatabricksDataType.STRING,
    Text: DatabricksDataType.STRING,
    Time: DatabricksDataType.STRING,
    Unicode: DatabricksDataType.STRING,
    UnicodeText: DatabricksDataType.STRING,
    Uuid: DatabricksDataType.STRING,
}

# Convert the dictionary into a list of tuples for use in pytest.mark.parametrize
_as_tuple_list = [(key, value) for key, value in camel_case_type_map.items()]


class CompilationTestBase:
    dialect = DatabricksDialect()

    def _assert_compiled_value(self, type_: TypeEngine, expected: DatabricksDataType):
        """Assert that when type_ is compiled for the databricks dialect, it renders the DatabricksDataType name.

        This method initialises the type_ with no arguments.
        """
        compiled_result = type_().compile(dialect=self.dialect)  # type: ignore
        assert compiled_result == expected.name

    def _assert_compiled_value_explicit(self, type_: TypeEngine, expected: str):
        """Assert that when type_ is compiled for the databricks dialect, it renders the expected string.

        This method expects an initialised type_ so that we can test how a TypeEngine created with arguments
        is compiled.
        """
        compiled_result = type_.compile(dialect=self.dialect)
        assert compiled_result == expected


class TestCamelCaseTypesCompilation(CompilationTestBase):
    """Per the sqlalchemy documentation[^1] here, the camel case members of sqlalchemy.types are
    are expected to work across all dialects. These tests verify that the types compile into valid
    Databricks SQL type strings. For example, the sqlalchemy.types.Integer() should compile as "INT".

    Truly custom types like STRUCT (notice the uppercase) are not expected to work across all dialects.
    We test these separately.

    Note that these tests have to do with type **name** compiliation. Which is separate from actually
    mapping values between Python and Databricks.

    Note: SchemaType and MatchType are not tested because it's not used in table definitions

    [1]: https://docs.sqlalchemy.org/en/20/core/type_basics.html#generic-camelcase-types
    """

    @pytest.mark.parametrize("type_, expected", _as_tuple_list)
    def test_bare_camel_case_types_compile(self, type_, expected):
        self._assert_compiled_value(type_, expected)

    def test_numeric_renders_as_decimal_with_precision(self):
        self._assert_compiled_value_explicit(Numeric(10), "DECIMAL(10)")

    def test_numeric_renders_as_decimal_with_precision_and_scale(self):
        self._assert_compiled_value_explicit(Numeric(10, 2), "DECIMAL(10, 2)")
