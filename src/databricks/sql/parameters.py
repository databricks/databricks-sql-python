import datetime
import decimal
from enum import Enum, auto
from typing import Dict, List, Optional, TypeVar, Union, Type, Any

from databricks.sql.exc import NotSupportedError
from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TSparkParameterValue,
)


class ParameterApproach(Enum):
    INLINE = 1
    NATIVE = 2
    NONE = 3


class ParameterStructure(Enum):
    NAMED = 1
    POSITIONAL = 2
    NONE = 3


class DatabricksSupportedType(Enum):
    """Enumerate every supported Databricks SQL type shown here:

    https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
    """

    BIGINT = auto()
    BINARY = auto()
    BOOLEAN = auto()
    DATE = auto()
    DECIMAL = auto()
    DOUBLE = auto()
    FLOAT = auto()
    INT = auto()
    INTERVAL = auto()
    VOID = auto()
    SMALLINT = auto()
    STRING = auto()
    TIMESTAMP = auto()
    TIMESTAMP_NTZ = auto()
    TINYINT = auto()
    ARRAY = auto()
    MAP = auto()
    STRUCT = auto()



TInferrable = Union[
    str, int, float, datetime.datetime, datetime.date, bool, decimal.Decimal, Type[None]
]

TAllowedParameterValue = Union[
    str,
    int,
    float,
    datetime.datetime,
    datetime.date,
    bool,
    decimal.Decimal,
    Type[None],
]


class DbSqlType(Enum):
    """The values of this enumeration are passed as literals to be used in a CAST
    evaluation by the thrift server.
    """

    STRING = "STRING"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    BOOLEAN = "BOOLEAN"
    INTERVAL_MONTH = "INTERVAL MONTH"
    INTERVAL_DAY = "INTERVAL DAY"
    VOID = "VOID"


class DbsqlParameterBase:
    """Parent class for IntegerParameter, DecimalParameter etc..

    Each each instance that extends this base class should be capable of generating a TSparkParameter
    It should know how to generate a cast expression based off its DatabricksSupportedType.

    By default the cast expression should render the string value of it's `value` and the literal
    name of its Databricks Supported Type

    Interface should be:

    from databricks.sql.parameters import DecimalParameter
    param = DecimalParameter(value, scale=None, precision=None)
    cursor.execute("SELECT ?",[param])

    Or

    from databricks.sql.parameters import IntegerParameter
    param = IntegerParameter(42)
    cursor.execute("SELECT ?", [param])
    """

    CAST_EXPR: str

    def __init__(self, value: Any, name: Optional[str] = None):
        self.value: TAllowedParameterValue = value
        self.name = name

    def as_tspark_param(self, named: bool) -> TSparkParameter:
        """Returns a TSparkParameter object that can be passed to the DBR thrift server."""

        tsp = TSparkParameter(value=self._tspark_param_value(), type=self._cast_expr())

        if named:
            tsp.name = self.name
            tsp.ordinal = False
        elif not named:
            tsp.ordinal = True
        return tsp

    def _tspark_param_value(self):
        return TSparkParameterValue(stringValue=str(self.value))

    def _cast_expr(self):
        return self.CAST_EXPR

    def __str__(self):
        return f"{type(self)}(name={self.name}, value={self.value}, type={self.type.value})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class IntegerParameter(DbsqlParameterBase):
    def __init__(self, value: int, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.INT.name


class StringParameter(DbsqlParameterBase):
    def __init__(self, value: str, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.STRING.name


class BigIntegerParameter(DbsqlParameterBase):
    def __init__(self, value: int, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.BIGINT.name


class BooleanParameter(DbsqlParameterBase):
    def __init__(self, value: bool, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.BOOLEAN.name


class DateParameter(DbsqlParameterBase):
    def __init__(self, value: datetime.date, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.DATE.name


class DoubleParameter(DbsqlParameterBase):
    def __init__(self, value: float, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.DOUBLE.name


class FloatParameter(DbsqlParameterBase):
    def __init__(self, value: float, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.FLOAT.name


class VoidParameter(DbsqlParameterBase):
    def __init__(self, value: Type[None], name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.VOID.name

    def _tspark_param_value(self):
        """For Void types, the TSparkParameter.value should be a Python NoneType"""
        return None


class SmallIntParameter(DbsqlParameterBase):
    def __init__(self, value: int, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.SMALLINT.name


class TimestampParameter(DbsqlParameterBase):
    def __init__(self, value: datetime.datetime, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.TIMESTAMP.name


class TimestampNTZParameter(DbsqlParameterBase):
    def __init__(self, value: datetime.datetime, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.TIMESTAMP_NTZ.name


class TinyIntParameter(DbsqlParameterBase):
    def __init__(self, value: int, name: Optional[str] = None):
        super().__init__(value=value, name=name)
    CAST_EXPR = DatabricksSupportedType.TINYINT.name


class DecimalParameter(DbsqlParameterBase):
    CAST_EXPR = "DECIMAL({},{})"
    def __init__(self, value: decimal.Decimal, name: Optional[str] = None):
        super().__init__(value=value, name=name)

    def __init__(
        self,
        value: decimal.Decimal,
        name: Optional[str] = None,
        scale: Optional[int] = None,
        precision: Optional[int] = None,
    ):
        super().__init__(value=value, name=name)
        self.value = value
        self.scale = scale
        self.precision = precision

        if not self.valid_scale_and_precision():
            raise ValueError(
                "DecimalParameter requires both or none of scale and precision to be set"
            )

    def valid_scale_and_precision(self):
        if (self.scale is None and self.precision is None) or (
            isinstance(self.scale, int) and isinstance(self.precision, int)
        ):
            return True
        else:
            return False

    def _cast_expr(self):
        if self.scale and self.precision:
            return self.CAST_EXPR.format(self.scale, self.precision)
        else:
            return self.calculate_decimal_cast_string(self.value)

    def calculate_decimal_cast_string(self, input: decimal.Decimal) -> str:
        """Returns the smallest SQL cast argument that can contain the passed decimal

        Example:
            Input:   Decimal("1234.5678")
            Output:  DECIMAL(8,4)
        """

        string_decimal = str(input)

        if string_decimal.startswith("0."):
            # This decimal is less than 1
            overall = after = len(string_decimal) - 2
        elif "." not in string_decimal:
            # This decimal has no fractional component
            overall = len(string_decimal)
            after = 0
        else:
            # This decimal has both whole and fractional parts
            parts = string_decimal.split(".")
            parts_lengths = [len(i) for i in parts]
            before, after = parts_lengths[:2]
            overall = before + after

        return self.CAST_EXPR.format(overall, after)



TDbsqlParameter = Union[
    Type[IntegerParameter],
    Type[StringParameter],
    Type[BigIntegerParameter],
    Type[BooleanParameter],
    Type[DateParameter],
    Type[DoubleParameter],
    Type[FloatParameter],
    Type[VoidParameter],
    Type[SmallIntParameter],
    Type[TimestampParameter],
    Type[TimestampNTZParameter],
    Type[TinyIntParameter],
    Type[DecimalParameter],
]

TDbsqlParameterReturn = Union[
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
    DecimalParameter,
]

_INFERENCE_TYPE_MAP = {
    str: StringParameter,
    float: FloatParameter,
    datetime.datetime: TimestampParameter,
    datetime.date: DateParameter,
    bool: BooleanParameter,
    decimal.Decimal: DecimalParameter,
    type(None): VoidParameter,
}


def dbsql_parameter_from_int(value: int, name: Optional[str] = None):
    """Returns IntegerParameter unless the passed int() requires a BIGINT.

    Note: TinyIntegerParameter is never inferred here because it is a rarely used type and clauses like LIMIT and OFFSET
    cannot accept TINYINT bound parameter values.
    """
    if -128 <= value <= 127:
        # If DBR is ever updated to permit TINYINT values passed to LIMIT and OFFSET
        # then we can change this line to return TinyIntParameter
        return IntegerParameter(value=value, name=name)
    elif -2147483648 <= value <= 2147483647:
        return IntegerParameter(value=value, name=name)
    else:
        return BigIntegerParameter(value=value, name=name)


def dbsql_parameter_from_primitive(
    value: TInferrable, name: Optional[str] = None
) -> TDbsqlParameterReturn:
    """Returns a DbsqlParameter subclass given an inferrable value

    This is a convenience function that can be used to create a DbsqlParameter subclass
    without having to explicitly import a subclass of DbsqlParameter.
    """

    t = type(value)
    fetched = _INFERENCE_TYPE_MAP.get(t)
    
    if fetched is not None:
        direct: TDbsqlParameter = fetched
        return direct(value=value, name=name)
    elif isinstance(value, int):
        return dbsql_parameter_from_int(value, name=name)

    raise NotSupportedError(
        f"Could not infer parameter type from value: {value} - {type(value)} \n"
        "Please specify the type explicitly."
    )


PrimitiveType = TypeVar(
    "PrimitiveType",
    str,
    int,
    float,
    datetime.datetime,
    datetime.date,
    bool,
    decimal.Decimal,
    Type[None],
)

TParameterList = Union[List[Type[DbsqlParameterBase]], List[PrimitiveType]]
TParameterDict = Dict[str, PrimitiveType]
TParameterCollection = Union[TParameterList, TParameterDict]


def calculate_decimal_cast_string(input: decimal.Decimal) -> str:
    """Returns the smallest SQL cast argument that can contain the passed decimal

    Example:
        Input:   Decimal("1234.5678")
        Output:  DECIMAL(8,4)
    """

    string_decimal = str(input)

    if string_decimal.startswith("0."):
        # This decimal is less than 1
        overall = after = len(string_decimal) - 2
    elif "." not in string_decimal:
        # This decimal has no fractional component
        overall = len(string_decimal)
        after = 0
    else:
        # This decimal has both whole and fractional parts
        parts = string_decimal.split(".")
        parts_lengths = [len(i) for i in parts]
        before, after = parts_lengths[:2]
        overall = before + after

    return f"DECIMAL({overall},{after})"
