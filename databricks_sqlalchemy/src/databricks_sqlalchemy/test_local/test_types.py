import enum

import pytest
import sqlalchemy

from databricks.sqlalchemy.base import DatabricksDialect
from databricks.sqlalchemy._types import TINYINT, TIMESTAMP, TIMESTAMP_NTZ


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
    sqlalchemy.types.BigInteger: DatabricksDataType.BIGINT,
    sqlalchemy.types.LargeBinary: DatabricksDataType.BINARY,
    sqlalchemy.types.Boolean: DatabricksDataType.BOOLEAN,
    sqlalchemy.types.Date: DatabricksDataType.DATE,
    sqlalchemy.types.DateTime: DatabricksDataType.TIMESTAMP_NTZ,
    sqlalchemy.types.Double: DatabricksDataType.DOUBLE,
    sqlalchemy.types.Enum: DatabricksDataType.STRING,
    sqlalchemy.types.Float: DatabricksDataType.FLOAT,
    sqlalchemy.types.Integer: DatabricksDataType.INT,
    sqlalchemy.types.Interval: DatabricksDataType.TIMESTAMP_NTZ,
    sqlalchemy.types.Numeric: DatabricksDataType.DECIMAL,
    sqlalchemy.types.PickleType: DatabricksDataType.BINARY,
    sqlalchemy.types.SmallInteger: DatabricksDataType.SMALLINT,
    sqlalchemy.types.String: DatabricksDataType.STRING,
    sqlalchemy.types.Text: DatabricksDataType.STRING,
    sqlalchemy.types.Time: DatabricksDataType.STRING,
    sqlalchemy.types.Unicode: DatabricksDataType.STRING,
    sqlalchemy.types.UnicodeText: DatabricksDataType.STRING,
    sqlalchemy.types.Uuid: DatabricksDataType.STRING,
}


def dict_as_tuple_list(d: dict):
    """Return a list of [(key, value), ...] from a dictionary."""
    return [(key, value) for key, value in d.items()]


class CompilationTestBase:
    dialect = DatabricksDialect()

    def _assert_compiled_value(
        self, type_: sqlalchemy.types.TypeEngine, expected: DatabricksDataType
    ):
        """Assert that when type_ is compiled for the databricks dialect, it renders the DatabricksDataType name.

        This method initialises the type_ with no arguments.
        """
        compiled_result = type_().compile(dialect=self.dialect)  # type: ignore
        assert compiled_result == expected.name

    def _assert_compiled_value_explicit(
        self, type_: sqlalchemy.types.TypeEngine, expected: str
    ):
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

    @pytest.mark.parametrize("type_, expected", dict_as_tuple_list(camel_case_type_map))
    def test_bare_camel_case_types_compile(self, type_, expected):
        self._assert_compiled_value(type_, expected)

    def test_numeric_renders_as_decimal_with_precision(self):
        self._assert_compiled_value_explicit(
            sqlalchemy.types.Numeric(10), "DECIMAL(10)"
        )

    def test_numeric_renders_as_decimal_with_precision_and_scale(self):
        self._assert_compiled_value_explicit(
            sqlalchemy.types.Numeric(10, 2), "DECIMAL(10, 2)"
        )


uppercase_type_map = {
    sqlalchemy.types.ARRAY: DatabricksDataType.ARRAY,
    sqlalchemy.types.BIGINT: DatabricksDataType.BIGINT,
    sqlalchemy.types.BINARY: DatabricksDataType.BINARY,
    sqlalchemy.types.BOOLEAN: DatabricksDataType.BOOLEAN,
    sqlalchemy.types.DATE: DatabricksDataType.DATE,
    sqlalchemy.types.DECIMAL: DatabricksDataType.DECIMAL,
    sqlalchemy.types.DOUBLE: DatabricksDataType.DOUBLE,
    sqlalchemy.types.FLOAT: DatabricksDataType.FLOAT,
    sqlalchemy.types.INT: DatabricksDataType.INT,
    sqlalchemy.types.SMALLINT: DatabricksDataType.SMALLINT,
    sqlalchemy.types.TIMESTAMP: DatabricksDataType.TIMESTAMP,
    TINYINT: DatabricksDataType.TINYINT,
    TIMESTAMP: DatabricksDataType.TIMESTAMP,
    TIMESTAMP_NTZ: DatabricksDataType.TIMESTAMP_NTZ,
}


class TestUppercaseTypesCompilation(CompilationTestBase):
    """Per the sqlalchemy documentation[^1], uppercase types are considered to be specific to some
    database backends. These tests verify that the types compile into valid Databricks SQL type strings.

    [1]: https://docs.sqlalchemy.org/en/20/core/type_basics.html#backend-specific-uppercase-datatypes
    """

    @pytest.mark.parametrize("type_, expected", dict_as_tuple_list(uppercase_type_map))
    def test_bare_uppercase_types_compile(self, type_, expected):
        if isinstance(type_, type(sqlalchemy.types.ARRAY)):
            # ARRAY cannot be initialised without passing an item definition so we test separately
            # I preserve it in the uppercase_type_map for clarity
            assert True
        else:
            self._assert_compiled_value(type_, expected)

    def test_array_string_renders_as_array_of_string(self):
        """SQLAlchemy's ARRAY type requires an item definition. And their docs indicate that they've only tested
        it with Postgres since that's the only first-class dialect with support for ARRAY.

        https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.ARRAY
        """
        self._assert_compiled_value_explicit(
            sqlalchemy.types.ARRAY(sqlalchemy.types.String), "ARRAY<STRING>"
        )
