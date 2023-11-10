from databricks.sql.parameters import (
    named_parameters_to_tsparkparams,
    infer_types,
    named_parameters_to_dbsqlparams_v1,
    named_parameters_to_dbsqlparams_v2,
    calculate_decimal_cast_string,
    DbsqlDynamicDecimalType,
)
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TSparkParameterValue,
    TSessionHandle,
    TOpenSessionResp,
)
from databricks.sql.utils import DbSqlParameter, DbSqlType
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


class TestTSparkParameterConversion(object):
    @pytest.mark.parametrize(
        "input_value, expected_type",
        [
            ("a", "STRING"),
            (1, "INTEGER"),
            (1000, "INTEGER"),
            (9223372036854775807, "BIGINT"),  # Max value of a signed 64-bit integer
            (True, "BOOLEAN"),
            (1.0, "FLOAT"),
        ],
    )
    def test_conversion_e2e(self, input_value, expected_type):
        """This behaviour falls back to Python's default string formatting of numbers"""
        output = named_parameters_to_tsparkparams([input_value])
        expected = TSparkParameter(
            name="",
            type=expected_type,
            value=TSparkParameterValue(stringValue=str(input_value)),
        )
        assert output == [expected]

    def test_conversion_e2e_decimal(self):
        input = DbSqlParameter(value="1.0", type=DbSqlType.DECIMAL)
        output = named_parameters_to_tsparkparams([input])
        assert output == [
            TSparkParameter(
                name="",
                type="DECIMAL(2,1)",
                value=TSparkParameterValue(stringValue="1.0"),
            )
        ]

    def test_basic_conversions_v1(self):
        # Test legacy codepath
        assert named_parameters_to_dbsqlparams_v1({"1": 1, "2": "foo", "3": 2.0}) == [
            DbSqlParameter("1", 1),
            DbSqlParameter("2", "foo"),
            DbSqlParameter("3", 2.0),
        ]

    def test_basic_conversions_v2(self):
        # Test interspersing named params with unnamed
        assert named_parameters_to_dbsqlparams_v2(
            [DbSqlParameter("1", 1.0, DbSqlType.DECIMAL), 5, DbSqlParameter("3", "foo")]
        ) == [
            DbSqlParameter("1", 1.0, DbSqlType.DECIMAL),
            DbSqlParameter("", 5),
            DbSqlParameter("3", "foo"),
        ]

    def test_infer_types_none(self):
        with pytest.raises(ValueError):
            infer_types([DbSqlParameter("", None)])

    def test_infer_types_dict(self):
        with pytest.raises(ValueError):
            infer_types([DbSqlParameter("", {1: 1})])

    @pytest.mark.parametrize(
        "input_value, expected_type",
        [
            (-128, DbSqlType.INTEGER),
            (127, DbSqlType.INTEGER),
            (-2147483649, DbSqlType.BIGINT),
            (-2147483648, DbSqlType.INTEGER),
            (2147483647, DbSqlType.INTEGER),
            (-9223372036854775808, DbSqlType.BIGINT),
            (9223372036854775807, DbSqlType.BIGINT),
        ],
    )
    def test_infer_types_integer(self, input_value, expected_type):
        input = DbSqlParameter("", input_value)
        output = infer_types([input])
        assert output == [
            DbSqlParameter("", str(input_value), expected_type)
        ], f"{output[0].type} received, expected {expected_type}"

    def test_infer_types_boolean(self):
        input = DbSqlParameter("", True)
        output = infer_types([input])
        assert output == [DbSqlParameter("", "True", DbSqlType.BOOLEAN)]

    def test_infer_types_float(self):
        input = DbSqlParameter("", 1.0)
        output = infer_types([input])
        assert output == [DbSqlParameter("", "1.0", DbSqlType.FLOAT)]

    def test_infer_types_string(self):
        input = DbSqlParameter("", "foo")
        output = infer_types([input])
        assert output == [DbSqlParameter("", "foo", DbSqlType.STRING)]

    def test_infer_types_decimal(self):
        # The output decimal will have a dynamically calculated decimal type with a value of DECIMAL(2,1)
        input = DbSqlParameter("", Decimal("1.0"))
        output: List[DbSqlParameter] = infer_types([input])

        x = output[0]

        assert x.value == "1.0"
        assert isinstance(x.type, DbsqlDynamicDecimalType)
        assert x.type.value == "DECIMAL(2,1)"

    def test_infer_types_none(self):
        input = DbSqlParameter("", None)
        output: List[DbSqlParameter] = infer_types([input])

        x = output[0]

        assert x.value == None
        assert x.type == DbSqlType.VOID
        assert x.type.value == "VOID"

    def test_infer_types_unsupported(self):
        class ArbitraryType:
            pass

        input = DbSqlParameter("", ArbitraryType())

        with pytest.raises(ValueError, match="Could not infer parameter type from"):
            infer_types([input])


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

class TestDbsqlParameter(object):

    combinations = [
        (Primitive.NONE, DbSqlType.VOID),
        (Primitive.BOOL, DbSqlType.BOOLEAN),
        (Primitive.INT, DbSqlType.INTEGER),
        (Primitive.STRING, DbSqlType.STRING),
        (Primitive.DECIMAL, DbSqlType.DECIMAL),
        (Primitive.DATE, DbSqlType.DATE),
        (Primitive.TIMESTAMP, DbSqlType.TIMESTAMP),
        (Primitive.DOUBLE, DbSqlType.FLOAT),
    ]

    @pytest.mark.parametrize("p, expected_type", combinations)
    def test_inferrence(self, p: Primitive, expected_type: DbSqlType):
        """Test that the type is inferred correctly"""
        dbsql_param = DbSqlParameter(value=p.value)
        dbsql_param.infer_type()
        assert dbsql_param.type == expected_type

