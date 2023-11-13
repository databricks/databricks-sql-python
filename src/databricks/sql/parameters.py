import datetime
import decimal
from enum import Enum, auto
from typing import Any, Dict, List, Optional, TypeVar, Union, Type, TypedDict
from dataclasses import dataclass

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


InferrableType = TypeVar(
    "InferrableType",
    str,
    int,
    float,
    datetime.datetime,
    datetime.date,
    bool,
    decimal.Decimal,
    type(None),
)


@dataclass
class CastExpression:
    literal: Union[None, str]
    as_expr: str


class BaseCastExpressionGenerator:
    def __init__(self, dbsql_supported_type: DatabricksSupportedType):
        self.default_cast_expr = dbsql_supported_type.name

    def tspark_value(self, value: InferrableType):
        """Return a TSparkParameterValue object given an inferrable value
        """

        return TSparkParameterValue(stringValue=str(value))

    def generate(self, dbsql_parameter: "DbsqlParameter") -> CastExpression:
        """Generate a cast expression given an instance of DbsqlParameter

        This method can be overridden by subclasses to provide custom casting logic based on
        the value of dbsql_parameter.
        """

        this_cast_expr = CastExpression(self.tspark_value(dbsql_parameter.value), self.default_cast_expr )

        return this_cast_expr


class DecimalCastExpressionGenerator(BaseCastExpressionGenerator):
    """Override the default cast expression for Decimal types"""

    def __init__(self):
        pass

    def generate(self, dbsql_parameter: "DbsqlParameter") -> CastExpression:
        literal = self.tspark_value(dbsql_parameter.value)
        cast_expr = calculate_decimal_cast_string(literal)

        this_cast_expr = CastExpression(literal, cast_expr)

        return this_cast_expr


class VoidCastExpressionGenerator(BaseCastExpressionGenerator):
    """Override the default cast expression for Void

    We don't send a value for VOID types, so we need to override the default behavior
    """

    def __init__(self):
        pass

    def generate(self, dbsql_parameter: "DbsqlParameter") -> CastExpression:
        literal = None
        cast_expr = DatabricksSupportedType.VOID.name

        this_cast_expr = CastExpression(literal, cast_expr)

        return this_cast_expr



class DbsqlParameterType(Enum):
    """These are the possible values that can be passed to DbsqlParameter.type

    Every name is a DatabricksSupportedType member name. This is a subset of DatabricksSupportedType
    since parameters are not supported for all types

    Every value is a sub-class of BaseCastExpressionGenerator
    """

    BIGINT = BaseCastExpressionGenerator(DatabricksSupportedType.BIGINT)
    BOOLEAN = BaseCastExpressionGenerator(DatabricksSupportedType.BOOLEAN)
    DATE = BaseCastExpressionGenerator(DatabricksSupportedType.DATE)
    DECIMAL = DecimalCastExpressionGenerator()
    DOUBLE = BaseCastExpressionGenerator(DatabricksSupportedType.DOUBLE)
    FLOAT = BaseCastExpressionGenerator(DatabricksSupportedType.FLOAT)
    INT = BaseCastExpressionGenerator(DatabricksSupportedType.INT)
    VOID = VoidCastExpressionGenerator()
    SMALLINT = BaseCastExpressionGenerator(DatabricksSupportedType.SMALLINT)
    STRING = BaseCastExpressionGenerator(DatabricksSupportedType.STRING)
    TIMESTAMP = BaseCastExpressionGenerator(DatabricksSupportedType.TIMESTAMP)
    TIMESTAMP_NTZ = BaseCastExpressionGenerator(DatabricksSupportedType.TIMESTAMP_NTZ)
    TINYINT = BaseCastExpressionGenerator(DatabricksSupportedType.TINYINT)



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


class DbsqlDynamicDecimalType:
    def __init__(self, value):
        self.value = value


class TypeMap:
    _TYPE_MAP = {
        str: DbsqlParameterType.STRING,
        int: DbsqlParameterType.INT,
        float: DbsqlParameterType.FLOAT,
        datetime.datetime: DbsqlParameterType.TIMESTAMP,
        datetime.date: DbsqlParameterType.DATE,
        bool: DbsqlParameterType.BOOLEAN,
        decimal.Decimal: DbsqlParameterType.DECIMAL,
        type(None): DbsqlParameterType.VOID,
    }

    def __init__(self):
        pass

    def get(self, value: InferrableType) -> DbsqlParameterType:
        
        if isinstance(value, int):
            return self.resolve_databricks_sql_integer_type(value)
        
        this_type = type(value)
        
        return self._TYPE_MAP.get(this_type)
    
    @staticmethod
    def resolve_databricks_sql_integer_type(integer):
        """Returns DbsqlParameterType.INTE unless the passed int() requires a BIGINT.

        Note: TINYINT is never inferred here because it is a rarely used type and clauses like LIMIT and OFFSET
        cannot accept TINYINT bound parameter values. If you need to bind a TINYINT value, you can explicitly
        declare its type in a DbsqlParameter object, which will bypass this inference logic.
        """
        if -128 <= integer <= 127:
            # If DBR is ever updated to permit TINYINT values passed to LIMIT and OFFSET
            # then we can change this line to return DbsqlParameterType.TINYINT
            return DbsqlParameterType.INT
        elif -2147483648 <= integer <= 2147483647:
            return DbsqlParameterType.INT
        else:
            return DbsqlParameterType.BIGINT

TYPE_MAP = {
    str: DbsqlParameterType.STRING,
    int: DbsqlParameterType.INT,
    float: DbsqlParameterType.FLOAT,
    datetime.datetime: DbsqlParameterType.TIMESTAMP,
    datetime.date: DbsqlParameterType.DATE,
    bool: DbsqlParameterType.BOOLEAN,
    decimal.Decimal: DbsqlParameterType.DECIMAL,
    type(None): DbsqlParameterType.VOID,
}


class DbsqlParameter:
    def __init__(
        self, name=None, value: InferrableType = None, type: Optional[DbsqlParameterType] = None
    ):
        self.name = name
        self.value = value
        self._type = type
        self._cast_expr: Union[str, None] = None
        self._type_map = TypeMap()

    @property
    def type(self) -> DbsqlParameterType:
        """The DbsqlParameterType of this parameter. If not set, it will be inferred from the value."""
        if self._type is None:
            self._infer_type()
        return self._type  # type: ignore

    @type.setter
    def type(self, value: Type[Enum]) -> None:
        self._type = value

    def _infer_type(self):
        known = self._type_map.get(self.value)
        if not known:
            raise NotSupportedError(
                f"Could not infer parameter type from value: {self.value} - {type(self.value)} \n"
                "Please specify the type explicitly."
            )
        self._type = known


    def as_tspark_param(self, named: bool) -> TSparkParameter:
        """Returns a TSparkParameter object that can be passed to the DBR thrift server."""

        cast_expression: CastExpression = self.type.value.generate(self)
        tsp = TSparkParameter(value=cast_expression.literal, type=cast_expression.as_expr)

        if named:
            tsp.name = self.name
            tsp.ordinal = False
        elif not named:
            tsp.ordinal = True
        return tsp

    def __str__(self):
        return f"DbsqlParameter(name={self.name}, value={self.value}, type={self.type.value})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


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

ListOfParameters = Union[List[DbsqlParameter], List[PrimitiveType]]
DictOfParameters = Dict[str, PrimitiveType]


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



