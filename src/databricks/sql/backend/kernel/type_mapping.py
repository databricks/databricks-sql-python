"""Arrow ↔ PEP 249 type translation for the kernel backend.

The kernel returns results as pyarrow ``Schema`` / ``RecordBatch``;
PEP 249 ``cursor.description`` is a list of 7-tuples with a
type-name string per column. ``description_from_arrow_schema``
flattens the conversion so ``KernelResultSet`` and any future
kernel-result wrapper share the same mapping.

The string constants come from ``SqlType`` in the SEA backend's
``conversion`` module — same single source of truth both backends
already use. The Arrow → ``SqlType`` lookup itself is kernel-
specific (SEA receives type-text from the server and normalises it;
the kernel receives Arrow schemas directly), so the mapping
function stays local but the names are shared.

Parameter binding (``TSparkParameter`` → kernel
``Statement.bind_param``) is handled by ``bind_tspark_params`` —
forwards the connector's already-string-encoded form to the kernel
binding without an intermediate Python-typed round-trip.
"""

from __future__ import annotations

from typing import Any, List, Tuple

import pyarrow

from databricks.sql.backend.sea.utils.conversion import SqlType
from databricks.sql.exc import NotSupportedError
from databricks.sql.thrift_api.TCLIService import ttypes

# Type names that the connector emits as compound TSparkParameter
# shapes (payload on ``arguments``, not ``value``). The kernel's
# parameter parser doesn't accept these yet, and our binding path
# only forwards ``value`` — so we reject them at the connector
# layer to avoid silently binding a typed NULL.
_COMPOUND_PARAM_TYPES = frozenset({"ARRAY", "MAP", "STRUCT"})


def _arrow_type_to_dbapi_string(arrow_type: pyarrow.DataType) -> str:
    """Map a pyarrow type to the Databricks SQL type name used in
    PEP 249 ``description``. Names come from ``SqlType`` so the
    kernel and SEA backends emit identical type-code strings;
    consumers can branch on them identically.
    """
    if pyarrow.types.is_boolean(arrow_type):
        return SqlType.BOOLEAN
    if pyarrow.types.is_int8(arrow_type):
        return SqlType.TINYINT
    if pyarrow.types.is_int16(arrow_type):
        return SqlType.SMALLINT
    if pyarrow.types.is_int32(arrow_type):
        return SqlType.INT
    if pyarrow.types.is_int64(arrow_type):
        return SqlType.BIGINT
    if pyarrow.types.is_float32(arrow_type):
        return SqlType.FLOAT
    if pyarrow.types.is_float64(arrow_type):
        return SqlType.DOUBLE
    if pyarrow.types.is_decimal(arrow_type):
        return SqlType.DECIMAL
    if pyarrow.types.is_string(arrow_type) or pyarrow.types.is_large_string(arrow_type):
        return SqlType.STRING
    if pyarrow.types.is_binary(arrow_type) or pyarrow.types.is_large_binary(arrow_type):
        return SqlType.BINARY
    if pyarrow.types.is_date(arrow_type):
        return SqlType.DATE
    if pyarrow.types.is_timestamp(arrow_type):
        return SqlType.TIMESTAMP
    if pyarrow.types.is_list(arrow_type) or pyarrow.types.is_large_list(arrow_type):
        return SqlType.ARRAY
    if pyarrow.types.is_struct(arrow_type):
        return SqlType.STRUCT
    if pyarrow.types.is_map(arrow_type):
        return SqlType.MAP
    # Fallback for types the kernel hasn't been observed to emit yet
    # (time32/time64, unsigned ints, dictionary, string_view,
    # binary_view, fixed_size_*). ``str(arrow_type)`` produces shapes
    # like ``"fixed_size_binary[16]"`` — distinguishable from the
    # canonical slugs above, so callers can detect the unknown.
    return str(arrow_type)


def description_from_arrow_schema(schema: pyarrow.Schema) -> List[Tuple]:
    """Build a PEP 249 ``description`` list from a pyarrow Schema.

    Each tuple is ``(name, type_code, display_size, internal_size,
    precision, scale, null_ok)``. PEP 249 allows ``null_ok`` to be
    either a bool or ``None``; the Thrift backend always reports
    ``None``, so we match that here for drop-in parity. The actual
    nullability bit is still available via ``schema.field(i).nullable``
    for callers that want it from the Arrow schema directly.

    ``type_code`` normally comes from the Arrow ``DataType`` via
    ``_arrow_type_to_dbapi_string``, which collapses
    Databricks-specific types into their nearest Arrow shape (e.g.
    ``VARIANT`` → ``Utf8``). To recover the precise Databricks type
    name, we consult the field's metadata first — the kernel writes
    the server-reported type into ``databricks.type_name`` (see
    ``databricks_sql_kernel::reader::metadata_keys``). Today only
    ``VARIANT`` is special-cased here for parity with the Thrift
    backend's behaviour; other precise types (``INTERVAL_*``,
    ``GEOMETRY``, ``GEOGRAPHY``) collapse to their Arrow shape on
    both backends and don't need a remap.
    """
    return [
        (
            field.name,
            _databricks_type_for_field(field),
            None,
            None,
            None,
            None,
            None,
        )
        for field in schema
    ]


def _databricks_type_for_field(field: pyarrow.Field) -> str:
    """Pick the PEP 249 type code for a single field.

    Consults the field's Arrow metadata under
    ``databricks.type_name`` (written by the kernel from the SEA
    response's column type) so types that collapse onto a generic
    Arrow shape can still be distinguished. Today only ``VARIANT``
    is mapped; everything else delegates to
    ``_arrow_type_to_dbapi_string``.
    """
    md = field.metadata or {}
    # `databricks.type_name` is bytes (Arrow metadata is always
    # bytes); compare against bytes to avoid one encode per field.
    if md.get(b"databricks.type_name") == b"VARIANT":
        return "variant"
    return _arrow_type_to_dbapi_string(field.type)


def _tspark_param_value_str(param: ttypes.TSparkParameter) -> Any:
    """Extract the string-encoded value from a ``TSparkParameter``,
    or ``None`` for SQL NULL.

    Native parameters (``IntegerParameter`` etc.) wrap their value
    in ``TSparkParameterValue(stringValue=str(self.value))``.
    ``VoidParameter._tspark_param_value()`` returns Python ``None``,
    so on the wire ``param.value`` is ``None`` and we surface that
    as ``None`` here.
    """
    if param.value is None:
        return None
    return param.value.stringValue


def bind_tspark_params(kernel_stmt, parameters: List[ttypes.TSparkParameter]) -> None:
    """Bind a list of ``TSparkParameter`` onto a kernel ``Statement``.

    The kernel expects positional bindings only (SEA v0 doesn't
    accept named bindings on the wire). The connector's
    ``TSparkParameter`` has an ``ordinal: bool`` flag; ``True`` means
    "treat as positional in source-list order". Named-binding
    parameters surface as ``NotSupportedError`` so the user gets a
    clear message instead of a server-side rejection.

    Compound types (``ARRAY`` / ``MAP`` / ``STRUCT``) build a
    ``TSparkParameter`` with the payload on ``arguments`` and
    ``value=None`` — forwarding that would silently bind a typed
    NULL. Reject up front with ``NotSupportedError`` so callers get
    a clear message instead of silent data loss.
    """
    for i, param in enumerate(parameters, start=1):
        # ``ordinal`` on connector-native params is a bool (True for
        # positional, False for named). Thrift defaults to ``None``;
        # treat any non-True value with a name as a named binding so
        # a future caller that forgets to set ordinal=True still gets
        # rejected instead of silently dropping the name.
        name = getattr(param, "name", None)
        if name and getattr(param, "ordinal", None) is not True:
            raise NotSupportedError(
                f"Named parameter binding (got name={name!r}) is not yet "
                "supported on the kernel backend; pass parameters positionally."
            )

        sql_type = param.type or "STRING"
        # Compound types put their payload on ``arguments``, not
        # ``value``. The kernel parser doesn't accept them yet, and
        # the binding path below only forwards ``value``. Detect
        # both the SQL-type name (handles ``"ARRAY"``, ``"MAP(...)"``,
        # ``"STRUCT<...>"``) and the presence of ``arguments`` so a
        # hand-rolled compound TSparkParameter is also caught.
        base_type = sql_type.split("(", 1)[0].split("<", 1)[0].upper()
        if base_type in _COMPOUND_PARAM_TYPES or getattr(param, "arguments", None):
            raise NotSupportedError(
                f"Compound parameter types (got {sql_type!r}) are not yet "
                "supported on the kernel backend."
            )

        value_str = _tspark_param_value_str(param)
        # The kernel takes 1-based ordinals; `i` is already that.
        # Errors from the kernel side (bad literal, unsupported type,
        # etc.) come up as KernelError and bubble through normally.
        kernel_stmt.bind_param(i, value_str, sql_type)
