import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Tuple, Union
from unittest.mock import Mock, patch, MagicMock
from databricks.sql import Error

from databricks.sql.thrift_api.TCLIService import ttypes
from contextlib import contextmanager

import pytz
import pytest

from databricks.sql.exc import NotSupportedError
from databricks.sql.utils import (
    DbSqlParameter,
    DbSqlType,
    calculate_decimal_cast_string,
    ParameterApproach,
)

from functools import wraps

from tests.e2e.test_driver import PySQLPytestTestCase


class MyCustomDecimalType(Enum):
    DECIMAL_38_0 = "DECIMAL(38,0)"
    DECIMAL_38_2 = "DECIMAL(38,2)"
    DECIMAL_18_9 = "DECIMAL(18,9)"


both_approaches = pytest.mark.parametrize(
    "approach", (ParameterApproach.INLINE, ParameterApproach.NATIVE)
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

    NATIVE_QUERY = "SELECT :p AS col"

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

    def _inline_roundtrip(self, params: dict):
        """This INSERT, SELECT, DELETE dance is necessary because simply selecting
        ```
        "SELECT %(param)s"
        ```
        in INLINE mode would always return a str and the nature of the test is to
        confirm that types are maintained.
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

    def _native_roundtrip(self, parameters: Union[Dict, List[Dict]]):
        with self.connection(extra_params={"use_inline_params": False}) as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.NATIVE_QUERY, parameters=parameters)
                return cursor.fetchone()

    def _get_one_result(self, approach: ParameterApproach, params):
        """When approach is INLINE then we use %(param)s paramstyle and a connection with use_inline_params=True
        When approach is NATIVE then we use :param paramstyle and a connection with use_inline_params=False
        """

        if approach == ParameterApproach.INLINE:
            return self._inline_roundtrip(params)
        elif approach == ParameterApproach.NATIVE:
            return self._native_roundtrip(params)

    def _quantize(self, input: Union[float, int], place_value=2) -> Decimal:
        return Decimal(str(input)).quantize(Decimal("0." + "0" * place_value))

    @pytest.mark.parametrize("explicit_inline", (True, False))
    def test_use_inline_by_default_with_warning(self, explicit_inline, caplog):
        """
        use_inline_params should be True by default. Warn the user if server supports native parameters.
        If a user explicitly sets use_inline_params, don't warn them about it.
        """

        extra_args = {"use_inline_params": True} if explicit_inline else {}

        with self.connection(extra_args) as conn:
            with conn.cursor() as cursor:
                with self.patch_server_supports_native_params(
                    supports_native_params=True
                ):
                    cursor.execute("SELECT %(p)s", parameters={"p": 1})
                    if explicit_inline:
                        assert (
                            "Consider using native parameters." not in caplog.text
                        ), "Log message should be suppressed"
                    else:
                        assert (
                            "Consider using native parameters." in caplog.text
                        ), "Log message should not be supressed"

    @both_approaches
    def test_primitive_inferred_none(self, approach: ParameterApproach, inline_table):
        params = {"p": None}
        result = self._get_one_result(approach, params)
        assert result.col == None

    @both_approaches
    def test_primitive_inferred_bool(self, approach: ParameterApproach, inline_table):
        params = {"p": True}
        result = self._get_one_result(approach, params)
        assert result.col == True

    @both_approaches
    def test_primitive_inferred_integer(
        self, approach: ParameterApproach, inline_table
    ):
        params = {"p": 1}
        result = self._get_one_result(approach, params)
        assert result.col == 1

    def test_primitive_inferred_double(self):
        params = {"p": 3.14}
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert self._quantize(result.col) == self._quantize(3.14)

    @both_approaches
    def test_primitive_inferred_date(self, approach: ParameterApproach, inline_table):
        # DATE in Databricks is mapped into a datetime.date object in Python
        date_value = datetime.date(2023, 9, 6)
        params = {"p": date_value}
        result = self._get_one_result(approach, params)
        assert result.col == date_value

    @both_approaches
    def test_primitive_inferred_timestamp(
        self, approach: ParameterApproach, inline_table
    ):
        # TIMESTAMP in Databricks is mapped into a datetime.datetime object in Python
        date_value = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
        params = {"p": date_value}
        result = self._get_one_result(approach, params)
        assert result.col == date_value

    @both_approaches
    def test_primitive_inferred_string(self, approach: ParameterApproach, inline_table):
        params = {"p": "Hello"}
        result = self._get_one_result(approach, params)
        assert result.col == "Hello"

    @both_approaches
    def test_primitive_inferred_decimal(
        self, approach: ParameterApproach, inline_table
    ):
        params = {"p": Decimal("1234.56")}
        result = self._get_one_result(approach, params)
        assert result.col == Decimal("1234.56")

    def test_dbsqlparam_inferred_none(self):
        params = [DbSqlParameter(name="p", value=None, type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == None

    def test_dbsqlparam_inferred_bool(self):
        params = [DbSqlParameter(name="p", value=True, type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == True

    def test_dbsqlparam_inferred_integer(self):
        params = [DbSqlParameter(name="p", value=1, type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == 1

    def test_dbsqlparam_inferred_double(self):
        params = [DbSqlParameter(name="p", value=3.14, type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert self._quantize(result.col) == self._quantize(3.14)

    def test_dbsqlparam_inferred_date(self):
        # DATE in Databricks is mapped into a datetime.date object in Python
        date_value = datetime.date(2023, 9, 6)
        params = [DbSqlParameter(name="p", value=date_value, type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == date_value

    def test_dbsqlparam_inferred_timestamp(self):
        # TIMESTAMP in Databricks is mapped into a datetime.datetime object in Python
        date_value = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
        params = [DbSqlParameter(name="p", value=date_value, type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == date_value

    def test_dbsqlparam_inferred_string(self):
        params = [DbSqlParameter(name="p", value="Hello", type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == "Hello"

    def test_dbsqlparam_inferred_decimal(self):
        params = [DbSqlParameter(name="p", value=Decimal("1234.56"), type=None)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == Decimal("1234.56")

    def test_dbsqlparam_explicit_none(self):
        params = [DbSqlParameter(name="p", value=None, type=DbSqlType.VOID)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == None

    def test_dbsqlparam_explicit_bool(self):
        params = [DbSqlParameter(name="p", value=True, type=DbSqlType.BOOLEAN)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == True

    def test_dbsqlparam_explicit_integer(self):
        params = [DbSqlParameter(name="p", value=1, type=DbSqlType.INTEGER)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == 1

    def test_dbsqlparam_explicit_double(self):
        params = [DbSqlParameter(name="p", value=3.14, type=DbSqlType.FLOAT)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert self._quantize(result.col) == self._quantize(3.14)

    def test_dbsqlparam_explicit_date(self):
        # DATE in Databricks is mapped into a datetime.date object in Python
        date_value = datetime.date(2023, 9, 6)
        params = [DbSqlParameter(name="p", value=date_value, type=DbSqlType.DATE)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == date_value

    def test_dbsqlparam_explicit_timestamp(self):
        # TIMESTAMP in Databricks is mapped into a datetime.datetime object in Python
        date_value = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
        params = [DbSqlParameter(name="p", value=date_value, type=DbSqlType.TIMESTAMP)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == date_value

    def test_dbsqlparam_explicit_string(self):
        params = [DbSqlParameter(name="p", value="Hello", type=DbSqlType.STRING)]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == "Hello"

    def test_dbsqlparam_explicit_decimal(self):
        params = [
            DbSqlParameter(name="p", value=Decimal("1234.56"), type=DbSqlType.DECIMAL)
        ]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == Decimal("1234.56")

    def test_dbsqlparam_custom_explicit_decimal_38_0(self):
        # This DECIMAL can be contained in a DECIMAL(38,0) column in Databricks
        value = Decimal("12345678912345678912345678912345678912")
        params = [
            DbSqlParameter(name="p", value=value, type=MyCustomDecimalType.DECIMAL_38_0)
        ]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == value

    def test_dbsqlparam_custom_explicit_decimal_38_2(self):
        # This DECIMAL can be contained in a DECIMAL(38,2) column in Databricks
        value = Decimal("123456789123456789123456789123456789.12")
        params = [
            DbSqlParameter(name="p", value=value, type=MyCustomDecimalType.DECIMAL_38_2)
        ]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == value

    def test_dbsqlparam_custom_explicit_decimal_18_9(self):
        # This DECIMAL can be contained in a DECIMAL(18,9) column in Databricks
        value = Decimal("123456789.123456789")
        params = [
            DbSqlParameter(name="p", value=value, type=MyCustomDecimalType.DECIMAL_18_9)
        ]
        result = self._get_one_result(ParameterApproach.NATIVE, params)
        assert result.col == value

    def test_calculate_decimal_cast_string(self):
        assert calculate_decimal_cast_string(Decimal("10.00")) == "DECIMAL(4,2)"
        assert (
            calculate_decimal_cast_string(
                Decimal("123456789123456789.123456789123456789")
            )
            == "DECIMAL(36,18)"
        )
