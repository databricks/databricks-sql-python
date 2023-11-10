import datetime
from contextlib import contextmanager
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Union
from unittest.mock import patch

from databricks.sql.exc import DatabaseError

import pytest
import pytz

from databricks.sql import Error
from databricks.sql.exc import NotSupportedError
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.utils import (
    TYPE_INFERRENCE_LOOKUP_TABLE,
    DbSqlParameter,
    DbSqlType,
    ParameterApproach,
    calculate_decimal_cast_string,
)
from tests.e2e.test_driver import PySQLPytestTestCase


class ParamStyle(Enum):
    NAMED = 1
    PYFORMAT = 2
    NONE = 3


class Primitive(Enum):
    """These are the inferrable types. This Enum is used for parametrized tests."""

    NONE = None
    BOOL = True
    INT = 1
    STRING = "Hello"
    DECIMAL = Decimal("1234.56")
    DATE = datetime.date(2023, 9, 6)
    TIMESTAMP = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
    DOUBLE = 3.14


class T(Enum):
    """This is a utility Enum for the explicit dbsqlparam tests"""

    DECIMAL_38_0 = "DECIMAL(38,0)"
    DECIMAL_38_2 = "DECIMAL(38,2)"
    DECIMAL_18_9 = "DECIMAL(18,9)"


# We don't test inline approach with named paramstyle because it's never supported
approach_paramstyle_combinations = [
    (ParameterApproach.INLINE, ParamStyle.PYFORMAT),
    (ParameterApproach.NATIVE, ParamStyle.PYFORMAT),
    (ParameterApproach.NATIVE, ParamStyle.NAMED),
]

# Each of these decimals requries the specified type string to be expressed in delta table
decimal_value_custom_type_combinations = [
    (Decimal("123456789.123456789"), T.DECIMAL_18_9),
    (Decimal("123456789123456789123456789123456789.12"), T.DECIMAL_38_2),
    (Decimal("12345678912345678912345678912345678912"), T.DECIMAL_38_0),
]

# This generates a list of tuples of (Primtive, DbSqlType)
primitive_dbsqltype_combinations = [
    (prim, TYPE_INFERRENCE_LOOKUP_TABLE.get(type(prim))) for prim in Primitive
]

both_paramstyles = pytest.mark.parametrize(
    "paramstyle", (ParamStyle.PYFORMAT, ParamStyle.NAMED)
)


class TestParameterizedQueries(PySQLPytestTestCase):
    """Namespace for tests of this connector's parameterisation behaviour.

    databricks-sql-connector can approach parameterisation in two ways:

        NATIVE: the connector will use server-side bound parameters implemented by DBR 14.1 and above.
        INLINE: the connector will render parameter values as strings and interpolate them into the query.

    Prior to connector version 3.0.0, the connector would always use the INLINE approach. This approach
    is still the default but this will be changed in a subsequent release.

    The INLINE and NATIVE approaches use different query syntax, which these tests verify.

    There is not 1-to-1 feature parity between these approaches. Where possible, we run the same test
    for @both_approaches.
    """

    NAMED_PARAMSTYLE_QUERY = "SELECT :p AS col"
    PYFORMAT_PARAMSTYLE_QUERY = "SELECT %(p)s AS col"

    inline_type_map = {
        int: "int_col",
        float: "float_col",
        Decimal: "decimal_col",
        str: "string_col",
        bool: "boolean_col",
        datetime.date: "date_col",
        datetime.datetime: "timestamp_col",
        type(None): "null_col",
    }

    @pytest.fixture(scope="class")
    def inline_table(self):
        """This table is necessary to verify that a parameter sent with INLINE
        approach can actually write to its analogous data type.

        For example, a Python Decimal(), when rendered inline, should be able
        to read/write into a DECIMAL column in Databricks

        Note that this fixture doesn't clean itself up. So the table will remain
        in the schema for use by subsequent test runs.
        """

        query = """
            CREATE TABLE IF NOT EXISTS pysql_e2e_inline_param_test_table (
            null_col INT,
            int_col INT,
            float_col FLOAT,
            decimal_col DECIMAL(10, 2),
            string_col STRING,
            boolean_col BOOLEAN,
            date_col DATE,
            timestamp_col TIMESTAMP
            ) USING DELTA
        """

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

    @contextmanager
    def patch_server_supports_native_params(self, supports_native_params: bool = True):
        """Applies a patch so we can test the connector's behaviour under different SPARK_CLI_SERVICE_PROTOCOL_VERSION conditions."""

        with patch(
            "databricks.sql.client.Connection.server_parameterized_queries_enabled",
            return_value=supports_native_params,
        ) as mock_parameterized_queries_enabled:
            try:
                yield mock_parameterized_queries_enabled
            finally:
                pass

    def _inline_roundtrip(self, params: dict, paramstyle: ParamStyle):
        """This INSERT, SELECT, DELETE dance is necessary because simply selecting
        ```
        "SELECT %(param)s"
        ```
        in INLINE mode would always return a str and the nature of the test is to
        confirm that types are maintained.

        :paramstyle:
            This is a no-op but is included to make the test-code easier to read.
        """
        target_column = self.inline_type_map[type(params.get("p"))]
        INSERT_QUERY = f"INSERT INTO pysql_e2e_inline_param_test_table (`{target_column}`) VALUES (%(p)s)"
        SELECT_QUERY = f"SELECT {target_column} `col` FROM pysql_e2e_inline_param_test_table LIMIT 1"
        DELETE_QUERY = "DELETE FROM pysql_e2e_inline_param_test_table"

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(INSERT_QUERY, parameters=params)
            with conn.cursor() as cursor:
                to_return = cursor.execute(SELECT_QUERY).fetchone()
            with conn.cursor() as cursor:
                cursor.execute(DELETE_QUERY)

        return to_return

    def _native_roundtrip(
        self,
        parameters: Union[Dict, List[Dict]],
        paramstyle: ParamStyle,
    ):
        if paramstyle == ParamStyle.NAMED:
            _query = self.NAMED_PARAMSTYLE_QUERY
        elif paramstyle == ParamStyle.PYFORMAT:
            _query = self.PYFORMAT_PARAMSTYLE_QUERY
        with self.connection(extra_params={"use_inline_params": False}) as conn:
            with conn.cursor() as cursor:
                cursor.execute(_query, parameters=parameters)
                return cursor.fetchone()

    def _get_one_result(
        self,
        params,
        approach: ParameterApproach = ParameterApproach.NONE,
        paramstyle: ParamStyle = ParamStyle.NONE,
    ):
        """When approach is INLINE then we use %(param)s paramstyle and a connection with use_inline_params=True
        When approach is NATIVE then we use :param paramstyle and a connection with use_inline_params=False
        """

        if approach == ParameterApproach.INLINE:
            # inline mode always uses ParamStyle.PYFORMAT
            return self._inline_roundtrip(params, paramstyle=ParamStyle.PYFORMAT)
        elif approach == ParameterApproach.NATIVE:
            # native mode can use either ParamStyle.NAMED or ParamStyle.PYFORMAT
            return self._native_roundtrip(params, paramstyle=paramstyle)

    def _quantize(self, input: Union[float, int], place_value=2) -> Decimal:
        return Decimal(str(input)).quantize(Decimal("0." + "0" * place_value))

    def _eq(self, actual, expected: Primitive):
        """This is a helper function to make the test code more readable.

        If primitive is Primitive.DOUBLE than an extra quantize step is performed before
        making the assertion.
        """
        if expected == Primitive.DOUBLE:
            return self._quantize(actual) == self._quantize(expected.value)

        return actual == expected.value

    @pytest.mark.parametrize("primitive", Primitive)
    @pytest.mark.parametrize("approach,paramstyle", approach_paramstyle_combinations)
    def test_primitive_with_inferrence(
        self, approach, paramstyle, primitive: Primitive
    ):
        """When ParameterApproach.INLINE is passed, inferrence will not be used.
        When ParameterApproach.NATIVE is passed, primitive inputs will be inferred.
        """

        params = {"p": primitive.value}
        result = self._get_one_result(params, approach, paramstyle)

        assert self._eq(result.col, primitive)

    @pytest.mark.parametrize("primitive", Primitive)
    def test_dbsqlparam_with_inferrence(self, primitive: Primitive):
        params = [DbSqlParameter(name="p", value=primitive.value, type=None)]
        result = self._get_one_result(params, ParameterApproach.NATIVE, ParamStyle.NAMED)
        assert self._eq(result.col, primitive)

    @pytest.mark.parametrize("primitive,dbsqltype", primitive_dbsqltype_combinations)
    def test_dbsqlparam_explicit(
        self, primitive: Primitive, dbsqltype: DbSqlType
    ):
        params = [DbSqlParameter(name="p", value=primitive.value, type=dbsqltype)]
        result = self._get_one_result(params, ParameterApproach.NATIVE, ParamStyle.NAMED)
        assert self._eq(result.col, primitive)

    @pytest.mark.parametrize("value, dbsqltype", decimal_value_custom_type_combinations)
    def test_dbsqlparam_custom_type(self, value, dbsqltype):
        params = [DbSqlParameter(name="p", value=value, type=dbsqltype)]
        result = self._get_one_result(
            params, ParameterApproach.NATIVE, ParamStyle.NAMED
        )
        assert result.col == value

    @pytest.mark.parametrize("explicit_inline", (True, False))
    def test_use_inline_by_default_with_warning(self, explicit_inline, caplog):
        """
        use_inline_params should be True by default. Warn the user if server supports native parameters.
        If a user explicitly sets use_inline_params, don't warn them about it.
        """

        extra_args = {"use_inline_params": True} if explicit_inline else {}

        with self.connection(extra_params=extra_args) as conn:
            with conn.cursor() as cursor:
                with self.patch_server_supports_native_params(
                    supports_native_params=True
                ):
                    cursor.execute("SELECT %(p)s", parameters={"p": 1})
                    if explicit_inline:
                        assert (
                            "Consider using native parameters." in caplog.text
                        ), "Log message should be suppressed"
                    else:
                        assert (
                            "Consider using native parameters." not in caplog.text
                        ), "Log message should not be supressed"


def test_calculate_decimal_cast_string():
    assert calculate_decimal_cast_string(Decimal("10.00")) == "DECIMAL(4,2)"
    assert (
        calculate_decimal_cast_string(Decimal("123456789123456789.123456789123456789"))
        == "DECIMAL(36,18)"
    )


class TestInlineParameterSyntax(PySQLPytestTestCase):
    """The inline parameter approach use s"""

    @pytest.mark.parametrize("use_inline_params", (True, False))
    def test_params_as_dict(self, use_inline_params):
        query = "SELECT %(foo)s foo, %(bar)s bar, %(baz)s baz"
        params = {"foo": 1, "bar": 2, "baz": 3}

        with self.connection(
            extra_params={"use_inline_params": use_inline_params}
        ) as conn:
            with conn.cursor() as cursor:
                result = cursor.execute(query, parameters=params).fetchone()

        assert result.foo == 1
        assert result.bar == 2
        assert result.baz == 3

    @pytest.mark.parametrize("use_inline_params", (True, False))
    def test_params_as_sequence(self, use_inline_params):
        """One side-effect of ParamEscaper using Python string interpolation to inline the values
        is that it can work with "ordinal" parameters, but only if a user writes parameter markers
        that are not defined with PEP-249. This test exists to prove that it works.

        But this test is expected to fail when using native approach because we haven't implemented
        ordinal parameters under the native approach (yet).
        """

        query = "SELECT %s foo, %s bar, %s baz"
        params = (1, 2, 3)

        with self.connection(
            extra_params={"use_inline_params": use_inline_params}
        ) as conn:
            with conn.cursor() as cursor:
                if not use_inline_params:
                    with pytest.raises(DatabaseError):
                        cursor.execute(query, parameters=params).fetchone()
                else:
                    result = cursor.execute(query, parameters=params).fetchone()
                    assert result.foo == 1
                    assert result.bar == 2
                    assert result.baz == 3
