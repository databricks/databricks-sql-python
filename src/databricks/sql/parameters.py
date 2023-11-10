from databricks.sql.thrift_api.TCLIService.ttypes import (
    TRowSet,
    TSparkArrowResultLink,
    TSparkParameter,
    TSparkParameterValue,
    TSparkRowSetType,
)

from enum import Enum
import datetime
import decimal
from typing import Any, Dict, List, Optional, Union

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

TYPE_INFERRENCE_LOOKUP_TABLE = {
    str: DbSqlType.STRING,
    int: DbSqlType.INTEGER,
    float: DbSqlType.FLOAT,
    datetime.datetime: DbSqlType.TIMESTAMP,
    datetime.date: DbSqlType.DATE,
    bool: DbSqlType.BOOLEAN,
    decimal.Decimal: DbSqlType.DECIMAL,
    type(None): DbSqlType.VOID,
}


from databricks.sql.exc import NotSupportedError

from typing import TypeVar
DbsqlParameterType = TypeVar("DbsqlParameterType", DbSqlType, DbsqlDynamicDecimalType,Enum)
class DbSqlParameter:
    name: str
    value: Any
    type: Union[DbSqlType, DbsqlDynamicDecimalType, Enum]

    def __init__(self, name="", value=None, type=None):
        self.name = name
        self.value = value
        self._type = type

    def infer_type(self):

        value_type = type(self.value)
        known = TYPE_INFERRENCE_LOOKUP_TABLE.get(value_type)
        if True or not known:
            raise NotSupportedError(f"Could not infer parameter type from value: {self.value} - {value_type}"
                                    "Please specify the type explicitly.")
        

        pass

    @property
    def type(self) -> DbsqlParameterType:
        if self._type is None:
            self._type = self.infer_type()
        return self._type
    
    @type.setter
    def type(self, value: DbsqlParameterType) -> None:
        self._type = value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

SupportedParameterType = TypeVar(
    "SupportedParameterType",
    DbSqlParameter,
    str,
    int,
    float,
    datetime.datetime,
    datetime.date,
    bool,
    decimal.Decimal,
    type(None),
)

ListOfParameters = List[SupportedParameterType]
DictOfParameters = Dict[str, SupportedParameterType]


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

def infer_types(params: list[DbSqlParameter]):
    new_params = []

    # cycle through each parameter we've been passed
    for param in params:
        _name: str = param.name
        _value: Any = param.value
        _type: Union[DbSqlType, DbsqlDynamicDecimalType, Enum, None]

        if param.type:
            _type = param.type
        else:
            # figure out what type to use
            _type = TYPE_INFERRENCE_LOOKUP_TABLE.get(type(_value), None)
            if not _type:
                raise ValueError(
                    f"Could not infer parameter type from {type(param.value)} - {param.value}"
                )

        # Decimal require special handling because one column type in Databricks can have multiple precisions
        if _type == DbSqlType.DECIMAL:
            cast_exp = calculate_decimal_cast_string(param.value)
            _type = DbsqlDynamicDecimalType(cast_exp)

        # int() requires special handling because one Python type can be cast to multiple SQL types (INT, BIGINT, TINYINT)
        if _type == DbSqlType.INTEGER:
            _type = resolve_databricks_sql_integer_type(param.value)

        # VOID / NULL types must be passed in a unique way as TSparkParameters with no value
        if _type == DbSqlType.VOID:
            new_params.append(DbSqlParameter(name=_name, type=DbSqlType.VOID))
            continue
        else:
            _value = str(param.value)

        new_params.append(DbSqlParameter(name=_name, value=_value, type=_type))

    return new_params

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


def named_parameters_to_tsparkparams(
    parameters: Union[List[Any], Dict[str, str]]
) -> List[TSparkParameter]:
    tspark_params = []
    if isinstance(parameters, dict):
        dbsql_params = named_parameters_to_dbsqlparams_v1(parameters)
    else:
        dbsql_params = named_parameters_to_dbsqlparams_v2(parameters)
    inferred_type_parameters = infer_types(dbsql_params)
    for param in inferred_type_parameters:
        # The only way to pass a VOID/NULL to DBR is to declare TSparkParameter without declaring
        # its value or type arguments. If we set these to NoneType, the request will fail with a
        # thrift transport error
        if param.type == DbSqlType.VOID:
            this_tspark_param = TSparkParameter(name=param.name)
        else:
            this_tspark_param_value = TSparkParameterValue(stringValue=param.value)
            this_tspark_param = TSparkParameter(
                type=param.type.value, name=param.name, value=this_tspark_param_value
            )
        tspark_params.append(this_tspark_param)
    return tspark_params


def named_parameters_to_dbsqlparams_v1(parameters: Dict[str, str]):
    dbsqlparams = []
    for name, parameter in parameters.items():
        dbsqlparams.append(DbSqlParameter(name=name, value=parameter))
    return dbsqlparams


def named_parameters_to_dbsqlparams_v2(parameters: List[Any]):
    dbsqlparams = []
    for parameter in parameters:
        if isinstance(parameter, DbSqlParameter):
            dbsqlparams.append(parameter)
        else:
            dbsqlparams.append(DbSqlParameter(value=parameter))
    return dbsqlparams


