import datetime
from decimal import Decimal
from enum import Enum
from typing import Type

import pytest
import pytz

from databricks.sql.client import Connection
from databricks.sql.parameters import (
    BigIntegerParameter,
    BooleanParameter,
    DateParameter,
    DecimalParameter,
    DoubleParameter,
    FloatParameter,
    IntegerParameter,
    SmallIntParameter,
    StringParameter,
    TimestampNTZParameter,
    TimestampParameter,
    TinyIntParameter,
    VoidParameter,
    MapParameter,
    ArrayParameter,
)
from databricks.sql.backend.types import SessionId
from databricks.sql.parameters.native import (
    TDbsqlParameter,
    TSparkParameter,
    TSparkParameterValue,
    TSparkParameterValueArg,
    dbsql_parameter_from_primitive,
)
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TOpenSessionResp,
    TSessionHandle,
    TSparkParameterValue,
)


class TestSessionHandleChecks(object):
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            (
                TOpenSessionResp(
                    serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                    sessionHandle=TSessionHandle(
                        sessionId=ttypes.THandleIdentifier(guid=0x36, secret=0x37),
                        serverProtocolVersion=None,
                    ),
                ),
                ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
            ),
            # Ensure that protocol version inside sessionhandle takes precedence.
            (
                TOpenSessionResp(
                    serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                    sessionHandle=TSessionHandle(
                        sessionId=ttypes.THandleIdentifier(guid=0x36, secret=0x37),
                        serverProtocolVersion=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
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
    ARRAY = [1, 2, 3]
    MAP = {"a": 1, "b": 2}


class TestDbsqlParameter:
    @pytest.mark.parametrize(
        "_type, prim, expect_cast_expr",
        (
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
            (MapParameter, Primitive.MAP, "MAP"),
            (ArrayParameter, Primitive.ARRAY, "ARRAY"),
        ),
    )
    def test_cast_expression(
        self, _type: TDbsqlParameter, prim: Primitive, expect_cast_expr: str
    ):
        p = _type(prim.value)
        assert p._cast_expr() == expect_cast_expr

    @pytest.mark.parametrize(
        "t, prim",
        (
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
        ),
    )
    def test_tspark_param_value(self, t: TDbsqlParameter, prim):
        p: TDbsqlParameter = t(prim.value)
        output = p._tspark_param_value()

        if prim == Primitive.NONE:
            assert output == None
        else:
            assert output == TSparkParameterValue(stringValue=str(prim.value))

    @pytest.mark.parametrize(
        "base_type,input,expected_output",
        [
            (
                ArrayParameter,
                [1, 2, 3],
                TSparkParameter(
                    ordinal=True,
                    name=None,
                    type="ARRAY",
                    value=None,
                    arguments=[
                        TSparkParameterValueArg(type="INT", value="1", arguments=None),
                        TSparkParameterValueArg(type="INT", value="2", arguments=None),
                        TSparkParameterValueArg(type="INT", value="3", arguments=None),
                    ],
                ),
            ),
            (
                MapParameter,
                {"a": 1, "b": 2},
                TSparkParameter(
                    ordinal=True,
                    name=None,
                    type="MAP",
                    value=None,
                    arguments=[
                        TSparkParameterValueArg(
                            type="STRING", value="a", arguments=None
                        ),
                        TSparkParameterValueArg(type="INT", value="1", arguments=None),
                        TSparkParameterValueArg(
                            type="STRING", value="b", arguments=None
                        ),
                        TSparkParameterValueArg(type="INT", value="2", arguments=None),
                    ],
                ),
            ),
            (
                ArrayParameter,
                [{"a": 1, "b": 2}, {"c": 3, "d": 4}],
                TSparkParameter(
                    ordinal=True,
                    name=None,
                    type="ARRAY",
                    value=None,
                    arguments=[
                        TSparkParameterValueArg(
                            type="MAP",
                            value=None,
                            arguments=[
                                TSparkParameterValueArg(
                                    type="STRING", value="a", arguments=None
                                ),
                                TSparkParameterValueArg(
                                    type="INT", value="1", arguments=None
                                ),
                                TSparkParameterValueArg(
                                    type="STRING", value="b", arguments=None
                                ),
                                TSparkParameterValueArg(
                                    type="INT", value="2", arguments=None
                                ),
                            ],
                        ),
                        TSparkParameterValueArg(
                            type="MAP",
                            value=None,
                            arguments=[
                                TSparkParameterValueArg(
                                    type="STRING", value="c", arguments=None
                                ),
                                TSparkParameterValueArg(
                                    type="INT", value="3", arguments=None
                                ),
                                TSparkParameterValueArg(
                                    type="STRING", value="d", arguments=None
                                ),
                                TSparkParameterValueArg(
                                    type="INT", value="4", arguments=None
                                ),
                            ],
                        ),
                    ],
                ),
            ),
        ],
    )
    def test_complex_type_tspark_param(self, base_type, input, expected_output):
        p = base_type(input)
        tsp = p.as_tspark_param()
        assert tsp == expected_output

    def test_tspark_param_named(self):
        p = dbsql_parameter_from_primitive(Primitive.INT.value, name="p")
        tsp = p.as_tspark_param(named=True)

        assert tsp.name == "p"
        assert tsp.ordinal is False

    def test_tspark_param_ordinal(self):
        p = dbsql_parameter_from_primitive(Primitive.INT.value, name="p")
        tsp = p.as_tspark_param(named=False)

        assert tsp.name is None
        assert tsp.ordinal is True

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
            (MapParameter, Primitive.MAP),
            (ArrayParameter, Primitive.ARRAY),
        ),
    )
    def test_inference(self, _type: TDbsqlParameter, prim: Primitive):
        """This method only tests inferrable types.

        Not tested are TinyIntParameter, SmallIntParameter DoubleParameter and TimestampNTZParameter
        """

        inferred_type = dbsql_parameter_from_primitive(prim.value)
        assert isinstance(inferred_type, _type)
