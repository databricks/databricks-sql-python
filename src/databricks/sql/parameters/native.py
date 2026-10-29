from __future__ import annotations

import datetime
import decimal
import warnings
from enum import Enum, auto
from typing import Optional, Sequence, Any, TYPE_CHECKING

from databricks.sql.exc import NotSupportedError

if TYPE_CHECKING:
    # Type-annotation-only imports. ``from __future__ import annotations`` keeps
    # these out of the runtime import graph; the ``TSparkParameter*`` objects
    # are constructed via function-local imports inside the ``as_tspark_param``
    # / ``_tspark_*`` helpers below, which only run on the Thrift execute path.
    # This keeps the Apache Thrift ``thrift`` package out of the SEA/kernel
    # load path (see ``test_lazy_thrift_import``).
    from databricks.sql.thrift_api.TCLIService.ttypes import (
        TSparkParameter,
        TSparkParameterValue,
        TSparkParameterValueArg,
    )

from typing import Dict, List, Union

# Names historically re-exported from this module that actually live in the
# Thrift-generated ``ttypes``. They are resolved lazily via ``__getattr__``
# (PEP 562) so ``from databricks.sql.parameters.native import TSparkParameter``
# keeps working for existing callers without importing the Apache Thrift
# ``thrift`` package at module load -- which is what keeps the SEA/kernel path
# Thrift-free. See ``test_lazy_thrift_import``.
_LAZY_THRIFT_REEXPORTS = frozenset(
    {"TSparkParameter", "TSparkParameterValue", "TSparkParameterValueArg"}
)


def __getattr__(name: str) -> Any:
    if name in _LAZY_THRIFT_REEXPORTS:
        from databricks.sql.thrift_api.TCLIService import ttypes

        return getattr(ttypes, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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


TAllowedParameterValue = Union[
    str,
    int,
    float,
    datetime.datetime,
    datetime.date,
    bool,
    decimal.Decimal,
    None,
    list,
    dict,
    tuple,
]


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
    name: Optional[str]
    value: Any

    def as_tspark_param(self, named: bool) -> TSparkParameter:
        """Returns a TSparkParameter object that can be passed to the DBR thrift server."""

        from databricks.sql.thrift_api.TCLIService.ttypes import TSparkParameter

        tsp = TSparkParameter(value=self._tspark_param_value(), type=self._cast_expr())

        if named:
            tsp.name = self.name
            tsp.ordinal = False
        elif not named:
            tsp.ordinal = True
        return tsp

    def _tspark_param_value(self):
        from databricks.sql.thrift_api.TCLIService.ttypes import (
            TSparkParameterValue,
        )

        return TSparkParameterValue(stringValue=str(self.value))

    def _tspark_value_arg(self):
        """Returns a TSparkParameterValueArg object that can be passed to the DBR thrift server."""
        from databricks.sql.thrift_api.TCLIService.ttypes import (
            TSparkParameterValueArg,
        )

        return TSparkParameterValueArg(value=str(self.value), type=self._cast_expr())

    def _cast_expr(self):
        return self.CAST_EXPR

    def __str__(self):
        return f"{self.__class__}(name={self.name}, value={self.value})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class IntegerParameter(DbsqlParameterBase):
    """Wrap a Python `int` that will be bound to a Databricks SQL INT column."""

    def __init__(self, value: int, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to an INT.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.INT.name


class StringParameter(DbsqlParameterBase):
    """Wrap a Python `str` that will be bound to a Databricks SQL STRING column."""

    def __init__(self, value: str, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a STRING.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.STRING.name


class BigIntegerParameter(DbsqlParameterBase):
    """Wrap a Python `int` that will be bound to a Databricks SQL BIGINT column."""

    def __init__(self, value: int, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a BIGINT.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.BIGINT.name


class BooleanParameter(DbsqlParameterBase):
    """Wrap a Python `bool` that will be bound to a Databricks SQL BOOLEAN column."""

    def __init__(self, value: bool, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a BOOLEAN.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.BOOLEAN.name


class DateParameter(DbsqlParameterBase):
    """Wrap a Python `date` that will be bound to a Databricks SQL DATE column."""

    def __init__(self, value: datetime.date, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a DATE.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.DATE.name


class DoubleParameter(DbsqlParameterBase):
    """Wrap a Python `float` that will be bound to a Databricks SQL DOUBLE column."""

    def __init__(self, value: float, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a DOUBLE.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.DOUBLE.name


class FloatParameter(DbsqlParameterBase):
    """Wrap a Python `float` that will be bound to a Databricks SQL FLOAT column."""

    def __init__(self, value: float, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a FLOAT.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.FLOAT.name


class VoidParameter(DbsqlParameterBase):
    """Wrap a Python `None` that will be bound to a Databricks SQL VOID type."""

    def __init__(self, value: None, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a VOID.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.VOID.name

    def _tspark_param_value(self):
        """For Void types, the TSparkParameter.value should be a Python NoneType"""
        return None


class SmallIntParameter(DbsqlParameterBase):
    """Wrap a Python `int` that will be bound to a Databricks SQL SMALLINT type."""

    def __init__(self, value: int, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a SMALLINT.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.SMALLINT.name


class TimestampParameter(DbsqlParameterBase):
    """Wrap a Python `datetime` that will be bound to a Databricks SQL TIMESTAMP type."""

    def __init__(self, value: datetime.datetime, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a TIMESTAMP.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.TIMESTAMP.name


class TimestampNTZParameter(DbsqlParameterBase):
    """Wrap a Python `datetime` that will be bound to a Databricks SQL TIMESTAMP_NTZ type."""

    def __init__(self, value: datetime.datetime, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a TIMESTAMP_NTZ.
            If it contains a timezone, that info will be lost.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.TIMESTAMP_NTZ.name


class TinyIntParameter(DbsqlParameterBase):
    """Wrap a Python `int` that will be bound to a Databricks SQL TINYINT type."""

    def __init__(self, value: int, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a TINYINT.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.value = value
        self.name = name

    CAST_EXPR = DatabricksSupportedType.TINYINT.name


class ArrayParameter(DbsqlParameterBase):
    """Wrap a Python `Sequence` that will be bound to a Databricks SQL ARRAY type."""

    def __init__(self, value: Sequence[Any], name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a ARRAY.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.name = name
        self.value = [dbsql_parameter_from_primitive(val) for val in value]

    def as_tspark_param(self, named: bool = False) -> TSparkParameter:
        """Returns a TSparkParameter object that can be passed to the DBR thrift server."""

        from databricks.sql.thrift_api.TCLIService.ttypes import TSparkParameter

        tsp = TSparkParameter(type=self._cast_expr())
        tsp.arguments = [val._tspark_value_arg() for val in self.value]

        if named:
            tsp.name = self.name
            tsp.ordinal = False
        elif not named:
            tsp.ordinal = True
        return tsp

    def _tspark_value_arg(self):
        """Returns a TSparkParameterValueArg object that can be passed to the DBR thrift server."""
        from databricks.sql.thrift_api.TCLIService.ttypes import (
            TSparkParameterValueArg,
        )

        tva = TSparkParameterValueArg(type=self._cast_expr())
        tva.arguments = [val._tspark_value_arg() for val in self.value]
        return tva

    CAST_EXPR = DatabricksSupportedType.ARRAY.name


class MapParameter(DbsqlParameterBase):
    """Wrap a Python `dict` that will be bound to a Databricks SQL MAP type."""

    def __init__(self, value: dict, name: Optional[str] = None):
        """
        :value:
            The value to bind for this parameter. This will be casted to a MAP.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        """
        self.name = name
        self.value = [
            dbsql_parameter_from_primitive(item)
            for key, val in value.items()
            for item in (key, val)
        ]

    def as_tspark_param(self, named: bool = False) -> TSparkParameter:
        """Returns a TSparkParameter object that can be passed to the DBR thrift server."""

        from databricks.sql.thrift_api.TCLIService.ttypes import TSparkParameter

        tsp = TSparkParameter(type=self._cast_expr())
        tsp.arguments = [val._tspark_value_arg() for val in self.value]
        if named:
            tsp.name = self.name
            tsp.ordinal = False
        elif not named:
            tsp.ordinal = True
        return tsp

    def _tspark_value_arg(self):
        """Returns a TSparkParameterValueArg object that can be passed to the DBR thrift server."""
        from databricks.sql.thrift_api.TCLIService.ttypes import (
            TSparkParameterValueArg,
        )

        tva = TSparkParameterValueArg(type=self._cast_expr())
        tva.arguments = [val._tspark_value_arg() for val in self.value]
        return tva

    CAST_EXPR = DatabricksSupportedType.MAP.name


class DecimalParameter(DbsqlParameterBase):
    """Wrap a Python `Decimal` that will be bound to a Databricks SQL DECIMAL type."""

    CAST_EXPR = "DECIMAL({},{})"

    def __init__(
        self,
        value: decimal.Decimal,
        name: Optional[str] = None,
        scale: Optional[int] = None,
        precision: Optional[int] = None,
    ):
        """
        If set, `scale` and `precision` must both be set. If neither is set, the value
        will be casted to the smallest possible DECIMAL type that can contain it.

        When set, `precision` and `scale` declare a *minimum* DECIMAL(precision, scale)
        type: if the value needs more fractional or integer digits than declared, the
        bound type is widened to fit it (capped at precision 38) so no digits are lost.
        The value is never rounded or truncated to a narrower declared type; round the
        `Decimal` yourself (e.g. with `Decimal.quantize`) before binding if you need that.

        :value:
            The value to bind for this parameter. This will be casted to a DECIMAL.
        :name:
            If None, your query must contain a `?` marker. Like:

            ```sql
               SELECT * FROM table WHERE field = ?
            ```
            If not None, your query should contain a named parameter marker. Like:
            ```sql
                SELECT * FROM table WHERE field = :my_param
            ```

            The `name` argument to this function would be `my_param`.
        :scale:
            The number of digits to the right of the decimal point. If the value itself
            has more fractional digits, the value's scale is used so no digits are lost
            (unless the value needs more than 38 digits in total; the excess fractional
            digits are then rounded by the server).
        :precision:
            The minimum precision (total number of digits) of the number, between 1 and 38.
            If the value has more integer digits than `precision - scale` allows, or its
            scale is widened, the precision grows to fit it (up to 38).
        """
        self.value: decimal.Decimal = value
        self.name = name
        self.scale = scale
        self.precision = precision

        if not self.valid_scale_and_precision():
            raise ValueError(
                "DecimalParameter requires both or none of scale and precision to be set"
            )

        if self.scale is not None and self.precision is not None and self.scale > self.precision:
            # Earlier releases documented these arguments the other way round
            # (scale = total digits, precision = fractional digits). A valid
            # DECIMAL can never have scale > precision, so this can only be a call
            # written for the old convention: keep it working for now (swap the
            # arguments) but warn so callers migrate before this becomes an error.
            warnings.warn(
                "DecimalParameter received scale ({}) > precision ({}); interpreting the "
                "arguments using the legacy (swapped) convention as DECIMAL({},{}). "
                "`precision` is the total number of digits and `scale` is the number of "
                "digits after the decimal point, as in SQL DECIMAL(precision, scale). "
                "Swap your arguments; this will raise ValueError in a future "
                "release.".format(self.scale, self.precision, self.scale, self.precision),
                # FutureWarning (not DeprecationWarning) so end users see it by
                # default, not only when the call originates from __main__.
                FutureWarning,
                stacklevel=2,
            )
            self.scale, self.precision = self.precision, self.scale

    def valid_scale_and_precision(self):
        if (self.scale is None and self.precision is None) or (
            isinstance(self.scale, int) and isinstance(self.precision, int)
        ):
            return True
        else:
            return False

    def _cast_expr(self):
        if self.scale is not None and self.precision is not None:
            precision, scale = self._effective_precision_and_scale(self.precision, self.scale)
            # SQL DECIMAL type syntax is DECIMAL(precision, scale)
            return self.CAST_EXPR.format(precision, scale)
        else:
            return self.calculate_decimal_cast_string(self.value)

    MAX_PRECISION = 38

    def _effective_precision_and_scale(self, precision: int, scale: int):
        """Widen a declared DECIMAL(precision, scale) so the bound value does not lose
        fractional digits: the value's own scale is authoritative over an
        under-declared target scale. Integer-digit room from the declared type is kept.

        Exception: the total is capped at MAX_PRECISION (38) digits. If the value
        itself needs more than 38 digits (e.g. a scale over 38), the cast is clamped
        and the server rounds away the fractional digits that do not fit.
        """
        try:
            sign, digits, exponent = self.value.as_tuple()
        except AttributeError:
            return precision, scale
        if not isinstance(exponent, int):
            # NaN / Infinity: leave the declared type as-is
            return precision, scale

        value_scale = max(-exponent, 0)
        value_int_digits = max(len(digits) + exponent, 0)

        desired_scale = max(scale, value_scale)
        desired_int_digits = max(precision - scale, value_int_digits)

        # Allocate the (at most MAX_PRECISION) digits by priority so that, when the
        # cap is hit, the value's own digits win over extra declared room
        # (similar to Spark's decimal precision adjustment):
        #   1. the value's integer digits, 2. the value's fractional digits,
        #   3. extra declared integer room, 4. extra declared fractional room.
        int_digits = min(value_int_digits, self.MAX_PRECISION)
        effective_scale = min(value_scale, self.MAX_PRECISION - int_digits)
        remaining = self.MAX_PRECISION - int_digits - effective_scale
        extra_int = min(desired_int_digits - int_digits, remaining)
        int_digits += extra_int
        remaining -= extra_int
        effective_scale += min(desired_scale - effective_scale, remaining)
        return int_digits + effective_scale, effective_scale

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
    value: TAllowedParameterValue, name: Optional[str] = None
) -> "TDbsqlParameter":
    """Returns a DbsqlParameter subclass given an inferrable value

    This is a convenience function that can be used to create a DbsqlParameter subclass
    without having to explicitly import a subclass of DbsqlParameter.
    """

    # This series of type checks are required for mypy not to raise
    # havoc. We can't use TYPE_INFERRENCE_MAP because mypy doesn't trust
    # its logic

    if isinstance(value, bool):
        return BooleanParameter(value=value, name=name)
    elif isinstance(value, int):
        return dbsql_parameter_from_int(value, name=name)
    elif isinstance(value, str):
        return StringParameter(value=value, name=name)
    elif isinstance(value, float):
        return DoubleParameter(value=value, name=name)
    elif isinstance(value, datetime.datetime):
        return TimestampParameter(value=value, name=name)
    elif isinstance(value, datetime.date):
        return DateParameter(value=value, name=name)
    elif isinstance(value, decimal.Decimal):
        return DecimalParameter(value=value, name=name)
    elif isinstance(value, dict):
        return MapParameter(value=value, name=name)
    elif isinstance(value, Sequence) and not isinstance(value, str):
        return ArrayParameter(value=value, name=name)
    elif value is None:
        return VoidParameter(value=value, name=name)
    else:
        raise NotSupportedError(
            f"Could not infer parameter type from value: {value} - {type(value)} \n"
            "Please specify the type explicitly."
        )


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
    ArrayParameter,
    MapParameter,
]


TParameterSequence = Sequence[Union[TDbsqlParameter, TAllowedParameterValue]]
TParameterDict = Dict[str, TAllowedParameterValue]
TParameterCollection = Union[TParameterSequence, TParameterDict]


_all__ = [
    "IntegerParameter",
    "StringParameter",
    "BigIntegerParameter",
    "BooleanParameter",
    "DateParameter",
    "DoubleParameter",
    "FloatParameter",
    "VoidParameter",
    "SmallIntParameter",
    "TimestampParameter",
    "TimestampNTZParameter",
    "TinyIntParameter",
    "DecimalParameter",
]
