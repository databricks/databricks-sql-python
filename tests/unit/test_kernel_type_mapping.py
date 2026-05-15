"""Unit tests for Arrow → PEP 249 description-string mapping."""

from __future__ import annotations

import pytest

# pyarrow is an optional connector dep; the default-deps CI test
# job runs without it. The kernel backend itself imports pyarrow
# at module load, so any test that touches the backend must skip
# when pyarrow is unavailable.
pa = pytest.importorskip("pyarrow")

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


# ─── bind_tspark_params ──────────────────────────────────────────────────


def _mk_param(*, type, value, ordinal=True, name=None):
    """Build a minimal TSparkParameter for tests."""
    from databricks.sql.thrift_api.TCLIService import ttypes

    p = ttypes.TSparkParameter(ordinal=ordinal, name=name, type=type)
    p.value = ttypes.TSparkParameterValue(stringValue=value) if value is not None else None
    return p


class _RecordingStmt:
    """Stand-in for the kernel `Statement` pyclass — records every
    `bind_param` call so tests can assert the (ordinal, value, type)
    triples the mapper forwarded."""

    def __init__(self):
        self.calls = []

    def bind_param(self, ordinal, value_str, sql_type):
        self.calls.append((ordinal, value_str, sql_type))


def test_bind_tspark_params_forwards_each_param_positionally():
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params

    params = [
        _mk_param(type="INT", value="42"),
        _mk_param(type="STRING", value="alice"),
        _mk_param(type="DATE", value="2026-05-15"),
    ]
    stmt = _RecordingStmt()
    bind_tspark_params(stmt, params)
    assert stmt.calls == [
        (1, "42", "INT"),
        (2, "alice", "STRING"),
        (3, "2026-05-15", "DATE"),
    ]


def test_bind_tspark_params_null_value():
    """TSparkParameter with value=None → kernel sees value_str=None,
    interpreted as SQL NULL regardless of the SQL type."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params

    p = _mk_param(type="STRING", value=None)
    stmt = _RecordingStmt()
    bind_tspark_params(stmt, [p])
    assert stmt.calls == [(1, None, "STRING")]


def test_bind_tspark_params_void_passes_through():
    """VoidParameter sets type='VOID' with stringValue='None'; the
    kernel parser ignores the value when type=VOID."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params

    p = _mk_param(type="VOID", value="None")
    stmt = _RecordingStmt()
    bind_tspark_params(stmt, [p])
    assert stmt.calls == [(1, "None", "VOID")]


def test_bind_tspark_params_named_param_rejected():
    """The kernel doesn't accept named bindings on the SEA wire;
    surface that at the connector layer so the user gets a pointed
    error instead of a server-side rejection."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params
    from databricks.sql.exc import NotSupportedError

    p = _mk_param(type="INT", value="42", ordinal=False, name="my_param")
    stmt = _RecordingStmt()
    with pytest.raises(NotSupportedError, match="(?i)named"):
        bind_tspark_params(stmt, [p])
    # Nothing should have been forwarded before the rejection.
    assert stmt.calls == []


def test_bind_tspark_params_missing_type_defaults_to_string():
    """Defensive: a TSparkParameter with no `type` shouldn't crash
    the mapper — fall back to STRING and let the kernel parse."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params
    from databricks.sql.thrift_api.TCLIService import ttypes

    p = ttypes.TSparkParameter(ordinal=True, name=None, type=None)
    p.value = ttypes.TSparkParameterValue(stringValue="hello")
    stmt = _RecordingStmt()
    bind_tspark_params(stmt, [p])
    assert stmt.calls == [(1, "hello", "STRING")]


def test_bind_tspark_params_empty_list_is_noop():
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params

    stmt = _RecordingStmt()
    bind_tspark_params(stmt, [])
    assert stmt.calls == []
