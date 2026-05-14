"""Unit tests for Arrow → PEP 249 description-string mapping."""

from __future__ import annotations

import pyarrow as pa
import pytest

from databricks.sql.backend.kernel.type_mapping import (
    _arrow_type_to_dbapi_string,
    description_from_arrow_schema,
)


@pytest.mark.parametrize(
    "arrow_type, expected",
    [
        (pa.bool_(), "boolean"),
        (pa.int8(), "tinyint"),
        (pa.int16(), "smallint"),
        (pa.int32(), "int"),
        (pa.int64(), "bigint"),
        (pa.float32(), "float"),
        (pa.float64(), "double"),
        (pa.decimal128(10, 2), "decimal"),
        (pa.string(), "string"),
        (pa.large_string(), "string"),
        (pa.binary(), "binary"),
        (pa.large_binary(), "binary"),
        (pa.date32(), "date"),
        (pa.timestamp("us"), "timestamp"),
        (pa.list_(pa.int32()), "array"),
        (pa.large_list(pa.int32()), "array"),
        (pa.struct([("a", pa.int32())]), "struct"),
        (pa.map_(pa.string(), pa.int32()), "map"),
    ],
)
def test_arrow_to_dbapi_known_types(arrow_type, expected):
    assert _arrow_type_to_dbapi_string(arrow_type) == expected


def test_arrow_to_dbapi_unknown_falls_back_to_str():
    # null type isn't in the explicit list but should fall through
    # to the default str() so unknown variants are still printable
    # rather than silently misclassified.
    assert _arrow_type_to_dbapi_string(pa.null()) == "null"


def test_description_from_schema_preserves_field_names_and_order():
    schema = pa.schema(
        [
            ("user_id", pa.int64()),
            ("name", pa.string()),
            ("created_at", pa.timestamp("us")),
        ]
    )
    desc = description_from_arrow_schema(schema)
    assert len(desc) == 3
    assert [(d[0], d[1]) for d in desc] == [
        ("user_id", "bigint"),
        ("name", "string"),
        ("created_at", "timestamp"),
    ]
    # PEP 249 says all 7-tuples; the last 5 slots are None for the
    # kernel backend (we don't report display_size / precision /
    # scale / nullability).
    for d in desc:
        assert len(d) == 7
        assert d[2:] == (None, None, None, None, None)
