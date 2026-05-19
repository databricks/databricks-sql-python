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
    # PEP 249 7-tuples; we report column name and type only. PEP 249
    # allows ``null_ok`` to be ``None`` and that's what the Thrift
    # backend has always returned; match it so kernel-backed cursors
    # are drop-in compatible. The Arrow ``field.nullable`` bit is still
    # available via ``schema.field(i).nullable`` for callers that need
    # the real value.
    for d in desc:
        assert len(d) == 7
        assert d[2:] == (None, None, None, None, None)


def test_description_null_ok_always_none_regardless_of_field_nullable():
    # Match Thrift backend's behaviour: ``null_ok`` is ``None`` for
    # every column even when the Arrow ``field.nullable`` bit is
    # meaningful.
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    desc = description_from_arrow_schema(schema)
    assert desc[0][6] is None
    assert desc[1][6] is None


def test_description_uses_databricks_type_name_for_variant():
    """VARIANT columns arrive over SEA as Arrow ``Utf8``; the kernel
    annotates them with ``databricks.type_name=VARIANT`` so the
    connector can recover the precise type for PEP-249 description.
    Matches the Thrift backend, which exposes the same column as
    ``variant``."""
    schema = pa.schema(
        [
            pa.field(
                "v",
                pa.string(),
                metadata={b"databricks.type_name": b"VARIANT"},
            ),
            # Plain Utf8 column without the metadata stays ``string``
            # so we don't claim "variant" for everything.
            pa.field("s", pa.string()),
        ]
    )
    desc = description_from_arrow_schema(schema)
    assert desc[0][1] == "variant"
    assert desc[1][1] == "string"


# ─── bind_tspark_params ──────────────────────────────────────────────────


def _mk_param(*, type, value, ordinal=True, name=None):
    """Build a minimal TSparkParameter for tests."""
    from databricks.sql.thrift_api.TCLIService import ttypes

    p = ttypes.TSparkParameter(ordinal=ordinal, name=name, type=type)
    p.value = (
        ttypes.TSparkParameterValue(stringValue=value) if value is not None else None
    )
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
    """VoidParameter._tspark_param_value() returns Python None, so
    on the wire ``param.value`` is None — the mapper forwards
    value_str=None with type='VOID' and the kernel parser ignores
    the value."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params

    p = _mk_param(type="VOID", value=None)
    stmt = _RecordingStmt()
    bind_tspark_params(stmt, [p])
    assert stmt.calls == [(1, None, "VOID")]


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


@pytest.mark.parametrize(
    "sql_type",
    ["ARRAY", "MAP", "STRUCT", "array", "Map(string,int)", "STRUCT<a:int>"],
)
def test_bind_tspark_params_compound_types_rejected(sql_type):
    """ArrayParameter / MapParameter / StructParameter build a
    TSparkParameter with value=None and the payload on
    ``arguments`` — forwarding that would silently bind a typed
    NULL, so reject up front."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params
    from databricks.sql.exc import NotSupportedError

    p = _mk_param(type=sql_type, value=None)
    stmt = _RecordingStmt()
    with pytest.raises(NotSupportedError, match="(?i)compound"):
        bind_tspark_params(stmt, [p])
    assert stmt.calls == []


def test_bind_tspark_params_arguments_field_rejected():
    """A TSparkParameter with ``arguments`` set is the compound
    shape regardless of how the type string looks — also reject."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params
    from databricks.sql.exc import NotSupportedError
    from databricks.sql.thrift_api.TCLIService import ttypes

    p = ttypes.TSparkParameter(ordinal=True, name=None, type="ARRAY")
    p.value = None
    p.arguments = [ttypes.TSparkParameterValueArg(type="INT")]
    stmt = _RecordingStmt()
    with pytest.raises(NotSupportedError, match="(?i)compound"):
        bind_tspark_params(stmt, [p])
    assert stmt.calls == []


def test_bind_tspark_params_named_with_ordinal_none_rejected():
    """Defensive: a TSparkParameter with a name and ordinal=None
    (Thrift default) should also be rejected as a named binding —
    not silently routed positionally with the name dropped."""
    from databricks.sql.backend.kernel.type_mapping import bind_tspark_params
    from databricks.sql.exc import NotSupportedError
    from databricks.sql.thrift_api.TCLIService import ttypes

    p = ttypes.TSparkParameter(ordinal=None, name="my_param", type="INT")
    p.value = ttypes.TSparkParameterValue(stringValue="42")
    stmt = _RecordingStmt()
    with pytest.raises(NotSupportedError, match="(?i)named"):
        bind_tspark_params(stmt, [p])
    assert stmt.calls == []
