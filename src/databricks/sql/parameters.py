import datetime
import decimal
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar, Union

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


TYPE_MAP = {
    str: DbSqlType.STRING,
    int: DbSqlType.INTEGER,
    float: DbSqlType.FLOAT,
    datetime.datetime: DbSqlType.TIMESTAMP,
    datetime.date: DbSqlType.DATE,
    bool: DbSqlType.BOOLEAN,
    decimal.Decimal: DbSqlType.DECIMAL,
    type(None): DbSqlType.VOID,
}


DbsqlParameterType = TypeVar(
    "DbsqlParameterType", DbSqlType, DbsqlDynamicDecimalType, Enum
)


class DbsqlParameter:
    def __init__(self, name=None, value=None, type=None):
        self.name = name
        self.value = value
        self._type = type

    @property
    def type(self) -> DbsqlParameterType:
        """The DbsqlParameterType of this parameter. If not set, it will be inferred from the value."""
        if self._type is None:
            self._infer_type()
        return self._type

    @type.setter
    def type(self, value: DbsqlParameterType) -> None:
        self._type = value

    def _infer_type(self):
        value_type = type(self.value)
        known = TYPE_MAP.get(value_type)
        if not known:
            raise NotSupportedError(
                f"Could not infer parameter type from value: {self.value} - {value_type} \n"
                "Please specify the type explicitly."
            )
        self._type = known
        self._process_type()

    def _process_type(self):
        """When self._type requires extra processing, this method performs that processing.
        Otherwise this method is a no-op.
        """

        if self._type == DbSqlType.DECIMAL:
            self._process_decimal()
        if self._type == DbSqlType.INTEGER:
            self._process_integer()
        if self._type == DbSqlType.VOID:
            self._process_void()

    def _process_decimal(self):
        """When self._type is DbSqlType.DECIMAL, this method calculates the smallest SQL cast argument that
        can contain self.value and assigns it to self._type.
        """

        cast_exp = calculate_decimal_cast_string(self.value)
        self._type = DbsqlDynamicDecimalType(cast_exp)

    def _process_integer(self):
        """int() requires special handling becone one Python type can be cast to multiple SQL types (INT, BIGINT, TINYINT)"""
        self._type = resolve_databricks_sql_integer_type(self.value)

    def _process_void(self):
        """VOID / NULL types must be passed in a unique way as TSparkParameters with no value"""
        self.value = None

    def as_tspark_param(self, named: bool) -> TSparkParameter:
        """Returns a TSparkParameter object that can be passed to the DBR thrift server."""
        if self.type == DbSqlType.VOID:
            tsp = TSparkParameter(value=None, type=self.type.value)
        else:
            tspark_param_value = TSparkParameterValue(stringValue=str(self.value))
            tsp = TSparkParameter(type=self.type.value, value=tspark_param_value)
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
    type(None),
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


def resolve_databricks_sql_integer_type(integer):
    """Returns DbsqlType.INTEGER unless the passed int() requires a BIGINT.

    Note: TINYINT is never inferred here because it is a rarely used type and clauses like LIMIT and OFFSET
    cannot accept TINYINT bound parameter values. If you need to bind a TINYINT value, you can explicitly
    declare its type in a DbsqlParameter object, which will bypass this inference logic.
    """
    if -128 <= integer <= 127:
        # If DBR is ever updated to permit TINYINT values passed to LIMIT and OFFSET
        # then we can change this line to return DbSqlType.TINYINT
        return DbSqlType.INTEGER
    elif -2147483648 <= integer <= 2147483647:
        return DbSqlType.INTEGER
    else:
        return DbSqlType.BIGINT
