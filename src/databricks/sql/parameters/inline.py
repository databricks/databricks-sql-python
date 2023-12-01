import datetime
import decimal
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from databricks.sql import exc
from databricks.sql.parameters.native import ParameterStructure, TDbsqlParameter

import logging

logger = logging.getLogger(__name__)

# Taken from PyHive
class ParamEscaper:
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise exc.ProgrammingError(
                "Unsupported param format: {}".format(parameters)
            )

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return "(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted)

    def escape_decimal(self, item):
        return str(item)

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, Iterable):
            return self.escape_sequence(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        elif isinstance(item, decimal.Decimal):
            return self.escape_decimal(item)
        else:
            raise exc.ProgrammingError("Unsupported object {}".format(item))


def inject_parameters(operation: str, parameters: Dict[str, str]):
    return operation % parameters


def _dbsqlparameter_names(params: List[TDbsqlParameter]) -> list[str]:
    return [p.name if p.name else "" for p in params]


def _generate_named_interpolation_values(
    params: List[TDbsqlParameter],
) -> dict[str, str]:
    """Returns a dictionary of the form {name: ":name"} for each parameter in params"""

    names = _dbsqlparameter_names(params)

    return {name: f":{name}" for name in names}


def _may_contain_inline_positional_markers(operation: str) -> bool:
    """Check for the presence of `%s` in the operation string."""

    interpolated = operation.replace("%s", "?")
    return interpolated != operation


def _interpolate_named_markers(
    operation: str, parameters: List[TDbsqlParameter]
) -> str:
    """Replace all instances of `%(param)s` in `operation` with `:param`.

    If `operation` contains no instances of `%(param)s` then the input string is returned unchanged.

    ```
    "SELECT * FROM table WHERE field = %(field)s and other_field = %(other_field)s"
    ```

    Yields

    ```
    SELECT * FROM table WHERE field = :field and other_field = :other_field
    ```
    """

    _output_operation = operation

    PYFORMAT_PARAMSTYLE_REGEX = r"%\((\w+)\)s"
    pat = re.compile(PYFORMAT_PARAMSTYLE_REGEX)
    NAMED_PARAMSTYLE_FMT = ":{}"
    PYFORMAT_PARAMSTYLE_FMT = "%({})s"

    pyformat_markers = pat.findall(operation)
    for marker in pyformat_markers:
        pyformat_marker = PYFORMAT_PARAMSTYLE_FMT.format(marker)
        named_marker = NAMED_PARAMSTYLE_FMT.format(marker)
        _output_operation = _output_operation.replace(pyformat_marker, named_marker)

    return _output_operation


def transform_paramstyle(
    operation: str,
    parameters: List[TDbsqlParameter],
    param_structure: ParameterStructure,
) -> str:
    """
    Performs a Python string interpolation such that any occurence of `%(param)s` will be replaced with `:param`

    This utility function is built to assist users in the transition between the default paramstyle in
    this connector prior to version 3.0.0 (`pyformat`) and the new default paramstyle (`named`).

    Args:
        operation: The operation or SQL text to transform.
        parameters: The parameters to use for the transformation.

    Returns:
        str
    """
    output = operation
    if (
        param_structure == ParameterStructure.POSITIONAL
        and _may_contain_inline_positional_markers(operation)
    ):
        logger.warning(
            "It looks like this query may contain un-named query markers like `%s`"
            " This format is not supported when use_inline_params=False."
            " Use `?` instead or set use_inline_params=True"
        )
    elif param_structure == ParameterStructure.NAMED:
        output = _interpolate_named_markers(operation, parameters)

    return output

NO_NATIVE_PARAMS: List = []

def prepare_inline_parameters(
    stmt: str, params: Optional[Union[Sequence, Dict[str, Any]]]
) -> Tuple[str, List]:
    """Return a statement and list of native parameters to be passed to thrift_backend for execution

    :stmt:
        A string SQL query containing parameter markers of PEP-249 paramstyle `pyformat`.
        For example `%(param)s`.

    :params:
        An iterable of parameter values to be rendered inline. If passed as a Dict, the keys
        must match the names of the markers included in :stmt:. If passed as a List, its length
        must equal the count of parameter markers in :stmt:.

    Returns a tuple of:
        stmt: the passed statement with the param markers replaced by literal rendered values
        params: an empty list representing the native parameters to be passed with this query.
            The list is always empty because native parameters are never used under the inline approach
    """

    escaped_values = ParamEscaper().escape_args(params)
    rendered_statement = inject_parameters(stmt, escaped_values)

    return rendered_statement, NO_NATIVE_PARAMS