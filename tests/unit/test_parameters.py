from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TSparkParameterValue,
    TSessionHandle,
    TOpenSessionResp,
)
import pytest

from databricks.sql.thrift_api.TCLIService import ttypes

from decimal import Decimal
from databricks.sql.client import Connection

from databricks.sql.parameters import (
    DecimalParameter,
    IntegerParameter,
    StringParameter,
    BigIntegerParameter,
    BooleanParameter,
    DateParameter,
    DoubleParameter,
    FloatParameter,
    VoidParameter,
    SmallIntParameter,
    TimestampParameter,
    TimestampNTZParameter,
    TinyIntParameter,
    TSparkParameterValue,
    TDbsqlParameter,
    dbsql_parameter_from_primitive,
)
from typing import Type


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


@pytest.mark.parametrize(
    "value,expected",
    (
        (Decimal("10.00"), "DECIMAL(4,2)"),
        (Decimal("123456789123456789.123456789123456789"), "DECIMAL(36,18)"),
        (Decimal(".12345678912345678912345678912345678912"), "DECIMAL(38,38)"),
        (Decimal("123456789.123456789"), "DECIMAL(18,9)"),
        (Decimal("12345678912345678912345678912345678912"), "DECIMAL(38,0)"),
        (Decimal("1234.56"), "DECIMAL(6,2)"),
    ),
)
def test_calculate_decimal_cast_string(value, expected):
    p = DecimalParameter(value)
    assert p._cast_expr() == expected


import datetime
from enum import Enum
import pytz


class Primitive(Enum):
    """These are the inferrable types. This Enum is used for parametrized tests."""

    NONE = None
    BOOL = True
    INT = 50
    BIGINT = 2147483648
    STRING = "Hello"
    DECIMAL = Decimal("1234.56")
    DATE = datetime.date(2023, 9, 6)
    TIMESTAMP = datetime.datetime(2023, 9, 6, 3, 14, 27, 843, tzinfo=pytz.UTC)
    DOUBLE = 3.14
    FLOAT = 3.15
    SMALLINT = 51


# class TestDbsqlParameter(object):
#     combinations = (
#         [Primitive.NONE, DbsqlParameterType.VOID],
#         [Primitive.INT, DbsqlParameterType.INT],
#         [Primitive.STRING, DbsqlParameterType.STRING],
#         [Primitive.DECIMAL, DbsqlParameterType.DECIMAL],
#         [Primitive.DATE, DbsqlParameterType.DATE],
#         [Primitive.TIMESTAMP, DbsqlParameterType.TIMESTAMP],
#         [Primitive.DOUBLE, DbsqlParameterType.FLOAT],
#         [Primitive.BOOL, DbsqlParameterType.BOOLEAN],
#     )

#     @pytest.mark.parametrize("p, expected_type", combinations)
#     def test_inferrence(self, p: Primitive, expected_type: DbSqlType):
#         """Test that the type is inferred correctly"""
#         value = p.value
#         dbsql_param = DbsqlParameter(value=value)
#         assert dbsql_param.type.name == expected_type.value

#     @pytest.mark.parametrize("named", [True, False])
#     @pytest.mark.parametrize("prim, expected_type", combinations)
#     def test_as_tspark_param(self, prim, expected_type, named: bool):
#         """Test that the generated TSparkParameter looks like what we expect

#         All TSparkParameterValues are sent as strings except for None.

#         For convenience, this test assumes that the type is inferred correctly
#         which is tested separately in test_inferrence. So if test_inferrence starts
#         to fail, this test will also fail.
#         """

#         p = DbsqlParameter(name="my_param", value=prim.value, type=expected_type)
#         tsp = p.as_tspark_param(named=named)

#         expected = TSparkParameter(
#             name="my_param" if named else None,
#             type=expected_type.name,
#             value=None if prim.value is None else TSparkParameterValue(str(prim.value)),
#             ordinal=not named,
#         )

#         assert tsp == expected


class TestDbsqlParameterNew:
    combinations = (
        (DecimalParameter, Primitive.DECIMAL, "DECIMAL(6,2)"),
        (IntegerParameter, Primitive.INT, "INT"),
        (StringParameter, Primitive.STRING, "STRING"),
        (BigIntegerParameter, Primitive.BIGINT, "BIGINT"),
        (BooleanParameter, Primitive.BOOL, "BOOLEAN"),
        (DateParameter, Primitive.DATE, "DATE"),
        (DoubleParameter, Primitive.DOUBLE, "DOUBLE"),
        (FloatParameter, Primitive.FLOAT, "FLOAT"),
        (VoidParameter, Primitive.NONE, "VOID"),
        (SmallIntParameter, Primitive.INT, "SMALLINT"),
        (TimestampParameter, Primitive.TIMESTAMP, "TIMESTAMP"),
        (TimestampNTZParameter, Primitive.TIMESTAMP, "TIMESTAMP_NTZ"),
        (TinyIntParameter, Primitive.INT, "TINYINT"),
    )

    @pytest.mark.parametrize("_type, prim, expect_cast_expr", combinations)
    def test_cast_expression(
        self, _type: TDbsqlParameter, prim: Primitive, expect_cast_expr: str
    ):
        p = _type(prim.value)
        assert p._cast_expr() == expect_cast_expr

    tspark_param_value_combinations = (
        (DecimalParameter, Primitive.DECIMAL),
        (IntegerParameter, Primitive.INT),
        (StringParameter, Primitive.STRING),
        (BigIntegerParameter, Primitive.BIGINT),
        (BooleanParameter, Primitive.BOOL),
        (DateParameter, Primitive.DATE),
        (DoubleParameter, Primitive.DOUBLE),
        (FloatParameter, Primitive.FLOAT),
        (VoidParameter, Primitive.NONE),
        (SmallIntParameter, Primitive.INT),
        (TimestampParameter, Primitive.TIMESTAMP),
        (TimestampNTZParameter, Primitive.TIMESTAMP),
        (TinyIntParameter, Primitive.INT),
    )

    @pytest.mark.parametrize("t, prim", tspark_param_value_combinations)
    def test_tspark_param_value(self, t: TDbsqlParameter, prim):
        p: TDbsqlParameter = t(prim.value)
        output = p._tspark_param_value()

        if prim == Primitive.NONE:
            assert output == None
        else:
            assert output == TSparkParameterValue(stringValue=str(prim.value))

    @pytest.mark.parametrize(
        "_type, prim",
        (
            (DecimalParameter, Primitive.DECIMAL),
            (IntegerParameter, Primitive.INT),
            (StringParameter, Primitive.STRING),
            (BigIntegerParameter, Primitive.BIGINT),
            (BooleanParameter, Primitive.BOOL),
            (DateParameter, Primitive.DATE),
            (FloatParameter, Primitive.FLOAT),
            (VoidParameter, Primitive.NONE),
            (TimestampParameter, Primitive.TIMESTAMP),
        ),
    )
    def test_inference(self, _type: TDbsqlParameter, prim: Primitive):
        """This method only tests inferrable types.

        Not tested are TinyIntParameter, SmallIntParameter DoubleParameter and TimestampNTZParameter
        """

        inferred_type = dbsql_parameter_from_primitive(prim.value)
        assert isinstance(inferred_type, _type)
