import datetime
import decimal
from enum import Enum, auto
from typing import Dict, List, Union, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sql.parameters.native import (
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
    )


IInferrable = Union[
    str, int, float, datetime.datetime, datetime.date, bool, decimal.Decimal, None
]

TInferrable = Union[
    Type[str],
    Type[int],
    Type[float],
    Type[datetime.datetime],
    Type[datetime.date],
    Type[bool],
    Type[decimal.Decimal],
    Type[None],
]

TAllowedParameterValue = Union[
    str, int, float, datetime.datetime, datetime.date, bool, decimal.Decimal, None
]

TDbsqlParameter = Union[
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

PrimitiveType = Union[
    str, int, float, datetime.datetime, datetime.date, bool, decimal.Decimal, None
]


TParameterList = List[Union[TDbsqlParameter, IInferrable]]
TParameterDict = Dict[str, PrimitiveType]
TParameterCollection = Union[TParameterList, TParameterDict]
