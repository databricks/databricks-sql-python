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

Parameter binding (``TSparkParameter`` → kernel ``TypedValue``) is
not yet implemented — the PyO3 ``Statement`` doesn't expose a
``bind_param`` method on this branch. It'll land in a follow-up
once that PyO3 surface ships.
"""

from __future__ import annotations

from typing import List, Tuple

import pyarrow

from databricks.sql.backend.sea.utils.conversion import SqlType


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
    precision, scale, null_ok)``. ``null_ok`` is taken from
    ``field.nullable``; the other four are not reported by the
    kernel today.
    """
    return [
        (
            field.name,
            _arrow_type_to_dbapi_string(field.type),
            None,
            None,
            None,
            None,
            field.nullable,
        )
        for field in schema
    ]
