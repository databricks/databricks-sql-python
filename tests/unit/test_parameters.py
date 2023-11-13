from databricks.sql.parameters import (
    calculate_decimal_cast_string,
    DbsqlDynamicDecimalType,
    BaseCastExpressionGenerator,
    TYPE_MAP,
    InferrableType,
    DbsqlParameterType
)
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TSparkParameterValue,
    TSessionHandle,
    TOpenSessionResp,
)
from databricks.sql.utils import DbsqlParameter, DbSqlType
import pytest

from databricks.sql.thrift_backend import ThriftBackend
from databricks.sql.thrift_api.TCLIService import ttypes

from decimal import Decimal
from databricks.sql.client import Connection
from typing import List


class TestSessionHandleChecks(object):
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            (
                TOpenSessionResp(
                    serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                    sessionHandle=TSessionHandle(1, None),
                ),
                ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
            ),
            # Ensure that protocol version inside sessionhandle takes precedence.
            (
                TOpenSessionResp(
                    serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                    sessionHandle=TSessionHandle(
                        1, ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8
                    ),
                ),
                ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
            ),
        ],
    )
    def test_get_protocol_version_fallback_behavior(self, test_input, expected):
        assert Connection.get_protocol_version(test_input) == expected

    @pytest.mark.parametrize(
        "test_input,expected",
        [
            (
                None,
                False,
            ),
            (
                ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                False,
            ),
            (
                ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
                True,
            ),
        ],
    )
    def test_parameters_enabled(self, test_input, expected):
        assert Connection.server_parameterized_queries_enabled(test_input) == expected


class TestCalculateDecimalCast(object):
    def test_38_38(self):
        input = Decimal(".12345678912345678912345678912345678912")
        output = calculate_decimal_cast_string(input)
        assert output == "DECIMAL(38,38)"

    def test_18_9(self):
        input = Decimal("123456789.123456789")
        output = calculate_decimal_cast_string(input)
        assert output == "DECIMAL(18,9)"

    def test_38_0(self):
        input = Decimal("12345678912345678912345678912345678912")
        output = calculate_decimal_cast_string(input)
        assert output == "DECIMAL(38,0)"

    def test_6_2(self):
        input = Decimal("1234.56")
        output = calculate_decimal_cast_string(input)
        assert output == "DECIMAL(6,2)"


import datetime
from enum import Enum
import pytz
from databricks.sql.utils import DbSqlType
from databricks.sql.parameters import DbsqlDynamicDecimalType, DbsqlParameterType, DatabricksSupportedType


class Primitive(Enum):
    """These are the inferrable types. This Enum is used for parametrized tests."""

    NONE = None
    BOOL = True
    INT = 50
    STRING = "Hello"
    DECIMAL = Decimal("1234.56")
    DATE = datetime.date(2023, 9, 6)
    TIMESTAMP = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
    DOUBLE = 3.14


class TestDbsqlParameter(object):
    combinations = (
        [Primitive.NONE, DbsqlParameterType.VOID],
        [Primitive.INT, DbsqlParameterType.INT],
        [Primitive.STRING, DbsqlParameterType.STRING],
        [Primitive.DECIMAL, DbsqlParameterType.DECIMAL],
        [Primitive.DATE, DbsqlParameterType.DATE],
        [Primitive.TIMESTAMP, DbsqlParameterType.TIMESTAMP],
        [Primitive.DOUBLE, DbsqlParameterType.FLOAT],
        [Primitive.BOOL, DbsqlParameterType.BOOLEAN],
    )

    @pytest.mark.parametrize("p, expected_type", combinations)
    def test_inferrence(self, p: Primitive, expected_type: DbSqlType):
        """Test that the type is inferred correctly"""
        value = p.value
        dbsql_param = DbsqlParameter(value=value)
        assert dbsql_param.type.name == expected_type.value

    @pytest.mark.parametrize("named", [True, False])
    @pytest.mark.parametrize("prim, expected_type", combinations)
    def test_as_tspark_param(self, prim, expected_type, named: bool):
        """Test that the generated TSparkParameter looks like what we expect

        All TSparkParameterValues are sent as strings except for None.

        For convenience, this test assumes that the type is inferred correctly
        which is tested separately in test_inferrence. So if test_inferrence starts
        to fail, this test will also fail.
        """

        p = DbsqlParameter(name="my_param", value=prim.value, type=expected_type)
        tsp = p.as_tspark_param(named=named)

        expected = TSparkParameter(
            name="my_param" if named else None,
            type=expected_type.name,
            value=None if prim.value is None else TSparkParameterValue(str(prim.value)),
            ordinal=not named
        )

        assert tsp == expected

class TestDbsqlParameterTypes(object):
    """These tests exist because Python isn't strongly typed.
    """

    def test_dbsql_parameter_types_are_allowed(self):
        """The DbsqlParameterType enumeration is frozen in code so that it can be auto-completed
        in a code editor. But it should never contain keys that are not a supported field type in Databricks."""
        
        supported_type_names = [i.name for i in DatabricksSupportedType]
        for t in DbsqlParameterType:
            assert t.name in supported_type_names

    def test_dbsql_parameter_type_values(self):
        """Every member of DbsqlParameterType's value should be a subclass of BaseCastExpressionGenerator
        """

        dbsql_parameter_type_values = [i.value for i in DbsqlParameterType]
        for t in dbsql_parameter_type_values:
            assert isinstance(t, BaseCastExpressionGenerator)

    def test_type_map(self):
        for t in TYPE_MAP.keys():
            assert isinstance(t, InferrableType)