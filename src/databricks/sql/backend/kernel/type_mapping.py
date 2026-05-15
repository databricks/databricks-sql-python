"""Arrow ↔ PEP 249 type translation for the kernel backend.

The kernel returns results as pyarrow ``Schema`` / ``RecordBatch``;
PEP 249 ``cursor.description`` is a list of 7-tuples with a
type-name string per column. ``description_from_arrow_schema``
flattens the conversion so ``KernelResultSet`` and any future
kernel-result wrapper share the same mapping.

Parameter binding (``TSparkParameter`` → kernel
``Statement.bind_param``) is handled by ``bind_tspark_params`` —
forwards the connector's already-string-encoded form to the kernel
binding without an intermediate Python-typed round-trip.
"""

from __future__ import annotations

from typing import Any, List, Tuple

import pyarrow

from databricks.sql.thrift_api.TCLIService import ttypes


def _arrow_type_to_dbapi_string(arrow_type: pyarrow.DataType) -> str:
    """Map a pyarrow type to the Databricks SQL type name used in
    PEP 249 ``description``. Names match what the Thrift backend
    produces so consumers can branch on them identically.
    """
    if pyarrow.types.is_boolean(arrow_type):
        return "boolean"
    if pyarrow.types.is_int8(arrow_type):
        return "tinyint"
    if pyarrow.types.is_int16(arrow_type):
        return "smallint"
    if pyarrow.types.is_int32(arrow_type):
        return "int"
    if pyarrow.types.is_int64(arrow_type):
        return "bigint"
    if pyarrow.types.is_float32(arrow_type):
        return "float"
    if pyarrow.types.is_float64(arrow_type):
        return "double"
    if pyarrow.types.is_decimal(arrow_type):
        return "decimal"
    if pyarrow.types.is_string(arrow_type) or pyarrow.types.is_large_string(arrow_type):
        return "string"
    if pyarrow.types.is_binary(arrow_type) or pyarrow.types.is_large_binary(arrow_type):
        return "binary"
    if pyarrow.types.is_date(arrow_type):
        return "date"
    if pyarrow.types.is_timestamp(arrow_type):
        return "timestamp"
    if pyarrow.types.is_list(arrow_type) or pyarrow.types.is_large_list(arrow_type):
        return "array"
    if pyarrow.types.is_struct(arrow_type):
        return "struct"
    if pyarrow.types.is_map(arrow_type):
        return "map"
    return str(arrow_type)


def description_from_arrow_schema(schema: pyarrow.Schema) -> List[Tuple]:
    """Build a PEP 249 ``description`` list from a pyarrow Schema.

    Each tuple is ``(name, type_code, display_size, internal_size,
    precision, scale, null_ok)``. The kernel does not report the
    last five so they're all ``None`` — same shape the existing
    ADBC / Thrift result paths produce.
    """
    return [
        (
            field.name,
            _arrow_type_to_dbapi_string(field.type),
            None,
            None,
            None,
            None,
            None,
        )
        for field in schema
    ]


def _tspark_param_value_str(param: ttypes.TSparkParameter) -> Any:
    """Extract the string-encoded value from a ``TSparkParameter``,
    or ``None`` for SQL NULL.

    Native parameters (``IntegerParameter`` etc.) always wrap their
    value in ``TSparkParameterValue(stringValue=str(self.value))``;
    ``VoidParameter`` sets ``stringValue="None"`` but the type is
    ``"VOID"`` — the kernel-side parser ignores the value when the
    type is VOID, so we don't have to special-case here.
    """
    if param.value is None:
        return None
    return param.value.stringValue


def bind_tspark_params(
    kernel_stmt, parameters: List[ttypes.TSparkParameter]
) -> None:
    """Bind a list of ``TSparkParameter`` onto a kernel ``Statement``.

    The kernel expects positional bindings only (SEA v0 doesn't
    accept named bindings on the wire). The connector's
    ``TSparkParameter`` has an ``ordinal: bool`` flag; ``True`` means
    "treat as positional in source-list order". Native bindings
    almost always come through positional today; named-binding
    parameters surface as ``NotSupportedError`` so the user gets a
    clear message instead of a server-side rejection.

    Compound types (``ARRAY`` / ``MAP`` / ``STRUCT``) are routed
    through the kernel parser which currently rejects them — same
    user-visible message ("compound parameter types … are not yet
    supported"). Tracked as a follow-up.
    """
    for i, param in enumerate(parameters, start=1):
        # The connector's `ordinal` field is a bool (True/False) on
        # native params and indicates positional vs named. Named
        # params can't flow through the kernel today; raise early
        # rather than letting the server reject.
        if getattr(param, "ordinal", None) is False and getattr(param, "name", None):
            from databricks.sql.exc import NotSupportedError

            raise NotSupportedError(
                f"Named parameter binding (got name={param.name!r}) is not yet "
                "supported on the kernel backend; pass parameters positionally."
            )

        sql_type = param.type or "STRING"
        value_str = _tspark_param_value_str(param)
        # The kernel takes 1-based ordinals; `i` is already that.
        # Errors from the kernel side (bad literal, unsupported type,
        # etc.) come up as KernelError and bubble through normally.
        kernel_stmt.bind_param(i, value_str, sql_type)
