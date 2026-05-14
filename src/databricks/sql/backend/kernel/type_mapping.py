"""Arrow ↔ PEP 249 type translation for the kernel backend.

The kernel returns results as pyarrow ``Schema`` / ``RecordBatch``;
PEP 249 ``cursor.description`` is a list of 7-tuples with a
type-name string per column. ``description_from_arrow_schema``
flattens the conversion so ``KernelResultSet`` and any future
kernel-result wrapper share the same mapping.

Parameter binding (``TSparkParameter`` → kernel ``TypedValue``) is
not yet implemented — the PyO3 ``Statement`` doesn't expose a
``bind_param`` method on this branch. It'll land in a follow-up
once that PyO3 surface ships.
"""

from __future__ import annotations

from typing import List, Tuple

import pyarrow


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
