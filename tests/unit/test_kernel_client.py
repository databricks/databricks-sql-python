"""Unit tests for ``KernelDatabricksClient`` — the error mapping,
state-mapping, async-handle bookkeeping, and method-level guards
that don't require a live kernel session.

The connector's ``databricks.sql.backend.kernel.client`` module
imports the ``databricks_sql_kernel`` extension at import time, so
this test installs a fake module into ``sys.modules`` *before*
importing the client. The fake exposes the minimum surface the
client touches (``Session``, ``KernelError``, ``Statement``,
``ExecutedStatement``, ``ExecutedAsyncStatement``, ``ResultStream``,
``metadata``).
"""

from __future__ import annotations

import sys
import types
from typing import Optional
from unittest.mock import MagicMock

import pytest

# pyarrow is an optional dep; the kernel client's result_set imports
# it eagerly, so the whole module must skip when pyarrow is missing.
pa = pytest.importorskip("pyarrow")


# ---------------------------------------------------------------------------
# Fake databricks_sql_kernel module — installed before client.py imports.
# ---------------------------------------------------------------------------


class _FakeKernelError(Exception):
    """Stand-in for ``databricks_sql_kernel.KernelError``. Carries
    the structured attrs the connector forwards onto the re-raised
    PEP 249 exception."""

    def __init__(
        self,
        code: str = "Unknown",
        message: str = "boom",
        sql_state: Optional[str] = None,
        query_id: Optional[str] = None,
        diagnostic_info: Optional[str] = None,
        display_message: Optional[str] = None,
        error_details_json: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.sql_state = sql_state
        self.error_code = None
        self.vendor_code = None
        self.http_status = None
        self.retryable = False
        self.query_id = query_id
        # Extended server status forwarded across the PyO3 boundary
        # (kernel #121). Defaults None so existing tests are unaffected.
        self.diagnostic_info = diagnostic_info
        self.display_message = display_message
        self.error_details_json = error_details_json


# These unit tests exercise the connector's error-mapping / wiring logic
# and need a *controllable* fake ``KernelError`` (to simulate arbitrary
# kernel error codes), so they install a fake ``databricks_sql_kernel``
# into ``sys.modules`` unconditionally.
#
# IMPORTANT: this fake is session-global and shadows a real wheel if one
# is installed. Tests that need the REAL wheel (the use_kernel routing
# test in test_session.py, and the e2e suite in
# tests/e2e/test_kernel_backend.py) MUST be run in a SEPARATE pytest
# invocation from this file — never `pytest tests/unit tests/e2e` in one
# session when the real wheel is installed. Both of those real-wheel
# tests detect the shadowing (real wheel present but sys.modules holds a
# stub) and FAIL LOUDLY rather than silently skipping, so a CI job that
# accidentally mixes them will go red instead of falsely green. The
# kernel CI matrix runs the real-wheel tests as their own step.
_fake_kernel_module = types.ModuleType("databricks_sql_kernel")
_fake_kernel_module.KernelError = _FakeKernelError  # type: ignore[attr-defined]
_fake_kernel_module.Session = MagicMock()  # type: ignore[attr-defined]
sys.modules.setdefault("databricks_sql_kernel", _fake_kernel_module)


# Importing the client now picks up the fake module via
# ``import databricks_sql_kernel as _kernel`` at the top of client.py.
from databricks.sql.auth.authenticators import AccessTokenAuthProvider
from databricks.sql.backend.kernel import client as kernel_client
from databricks.sql.backend.types import CommandId, CommandState
from databricks.sql.exc import (
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    ServerOperationError,
)

# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code, expected_cls",
    [
        ("InvalidArgument", ProgrammingError),
        ("Unauthenticated", OperationalError),
        ("PermissionDenied", OperationalError),
        ("NotFound", ProgrammingError),
        ("ResourceExhausted", OperationalError),
        ("Unavailable", OperationalError),
        ("Timeout", OperationalError),
        ("Cancelled", OperationalError),
        ("DataLoss", DatabaseError),
        ("Internal", DatabaseError),
        ("InvalidStatementHandle", ProgrammingError),
        ("NetworkError", OperationalError),
        # `SqlError` is the kernel's slug for server-side query
        # failures (syntax error, missing object, etc.) — exactly the
        # case Thrift's backend surfaces as ``ServerOperationError``.
        # Match Thrift so user code that catches the specific class
        # works equivalently. ``ServerOperationError`` is itself a
        # ``DatabaseError`` subclass, so existing catches of the base
        # class are unaffected.
        ("SqlError", ServerOperationError),
        ("Unknown", DatabaseError),
    ],
)
def test_code_to_exception_mapping(code, expected_cls):
    """Every entry in ``_CODE_TO_EXCEPTION`` maps to the documented
    PEP 249 class. Cause chaining happens at the ``raise ... from exc``
    call site, not inside ``_reraise_kernel_error`` — verified
    separately by ``test_kernel_error_chains_through_wrap``."""
    err = _FakeKernelError(code=code, message=f"{code} boom")
    out = kernel_client._reraise_kernel_error(err)
    assert isinstance(out, expected_cls)
    assert "boom" in str(out)


def test_unknown_code_falls_back_to_database_error():
    err = _FakeKernelError(code="SomethingNew", message="…")
    out = kernel_client._reraise_kernel_error(err)
    assert isinstance(out, DatabaseError)


def test_reraise_forwards_structured_attributes():
    err = _FakeKernelError(
        code="SqlError",
        message="table not found",
        sql_state="42P01",
        query_id="q-123",
    )
    out = kernel_client._reraise_kernel_error(err)
    assert out.code == "SqlError"
    assert out.sql_state == "42P01"
    assert out.query_id == "q-123"
    # Optional fields default to None on the source exception and
    # come through verbatim on the re-raised side.
    for attr in ("error_code", "vendor_code", "http_status"):
        assert getattr(out, attr) is None
    assert out.retryable is False


def test_reraise_forwards_extended_status_attributes():
    """display_message / diagnostic_info / error_details_json now cross
    the PyO3 boundary (kernel #121) and must be forwarded onto the
    re-raised exception so callers can read them."""
    err = _FakeKernelError(
        code="SqlError",
        message="boom",
        diagnostic_info="org.apache.spark...stack",
        display_message="user-facing msg",
        error_details_json='{"k":1}',
    )
    out = kernel_client._reraise_kernel_error(err)
    assert out.diagnostic_info == "org.apache.spark...stack"
    assert out.display_message == "user-facing msg"
    assert out.error_details_json == '{"k":1}'


def test_server_operation_error_populates_context_like_thrift():
    """A SqlError maps to ServerOperationError; its ``context`` must
    carry ``diagnostic-info`` (the Spark stack trace) and
    ``operation-id``, matching the Thrift backend so callers reading
    ``err.context["diagnostic-info"]`` work identically on use_kernel."""
    err = _FakeKernelError(
        code="SqlError",
        message="table not found",
        query_id="q-123",
        diagnostic_info="org.apache.spark...stack",
    )
    out = kernel_client._reraise_kernel_error(err)
    assert isinstance(out, ServerOperationError)
    assert out.context["diagnostic-info"] == "org.apache.spark...stack"
    assert out.context["operation-id"] == "q-123"


def test_kernel_error_chains_through_wrap():
    """``raise wrap_kernel_exception(...) from exc`` is the call-site
    pattern; ``__cause__`` must be set to the original ``KernelError``
    so users can dig out the structured fields via ``e.__cause__``."""
    src = _FakeKernelError(code="SqlError", message="boom", sql_state="42P01")
    try:
        try:
            raise src
        except Exception as exc:
            from databricks.sql.backend.kernel._errors import wrap_kernel_exception

            raise wrap_kernel_exception("test_site", exc) from exc
    except DatabaseError as out:
        assert out.__cause__ is src
        assert getattr(out, "sql_state", None) == "42P01"
    else:
        raise AssertionError("expected DatabaseError")


# ---------------------------------------------------------------------------
# State mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kernel_state, expected",
    [
        ("Pending", CommandState.PENDING),
        ("Running", CommandState.RUNNING),
        ("Succeeded", CommandState.SUCCEEDED),
        ("Failed", CommandState.FAILED),
        ("Cancelled", CommandState.CANCELLED),
        ("Closed", CommandState.CLOSED),
    ],
)
def test_state_to_command_state_mapping(kernel_state, expected):
    assert kernel_client._STATE_TO_COMMAND_STATE[kernel_state] == expected


# ---------------------------------------------------------------------------
# Client lifecycle / guards (no live session)
# ---------------------------------------------------------------------------


def _make_client() -> kernel_client.KernelDatabricksClient:
    """Build a client with a PAT auth provider; the kernel ``Session``
    isn't opened until ``open_session`` runs."""
    return kernel_client.KernelDatabricksClient(
        server_hostname="example.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/abc",
        auth_provider=AccessTokenAuthProvider("dapi-test"),
        ssl_options=None,
    )


def test_no_open_session_guards_raise_interface_error():
    """Every method that depends on an open kernel session must
    raise ``InterfaceError`` before any kernel call."""
    c = _make_client()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    with pytest.raises(InterfaceError, match="open session"):
        c.execute_command(
            operation="SELECT 1",
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )

    for method, kwargs in [
        ("get_catalogs", {}),
        ("get_schemas", {}),
        ("get_tables", {}),
        ("get_columns", {"catalog_name": "main"}),
    ]:
        with pytest.raises(InterfaceError):
            getattr(c, method)(
                session_id=MagicMock(),
                max_rows=1,
                max_bytes=1,
                cursor=cursor,
                **kwargs,
            )

    # The async-state methods attach to the kernel session by id; they
    # must also guard a closed/absent session before the kernel call.
    cid = CommandId.from_sea_statement_id("any-id")
    with pytest.raises(InterfaceError):
        c.get_query_state(cid)
    with pytest.raises(InterfaceError):
        c.get_execution_result(cid, cursor=cursor)


def test_open_session_rejects_double_open(monkeypatch):
    """Two ``open_session`` calls on the same client must fail —
    the kernel session is bound to a single open call."""
    c = _make_client()
    c._kernel_session = MagicMock()  # pretend already open
    with pytest.raises(InterfaceError, match="already has an open session"):
        c.open_session(session_configuration=None, catalog=None, schema=None)


@pytest.mark.parametrize(
    "kwargs, expected_flag",
    [
        ({}, False),  # default → arrow-native → kernel JSON off
        ({"_use_arrow_native_complex_types": True}, False),
        ({"_use_arrow_native_complex_types": False}, True),
    ],
)
def test_open_session_passes_complex_types_as_json_to_kernel(
    monkeypatch, kwargs, expected_flag
):
    """``_use_arrow_native_complex_types=False`` flips the kernel's
    ``complex_types_as_json`` post-processor on; the default and
    explicit ``True`` both leave it off. The flag is inverted at the
    boundary because the connector's option is "native Arrow"-shaped
    and the kernel's is "rewrite to JSON strings"-shaped."""
    captured = {}

    def fake_session(**kw):
        captured.update(kw)
        sess = MagicMock()
        sess.session_id = "sess-id"
        return sess

    monkeypatch.setattr(kernel_client._kernel, "Session", fake_session)

    c = kernel_client.KernelDatabricksClient(
        server_hostname="example.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/abc",
        auth_provider=AccessTokenAuthProvider("dapi-test"),
        ssl_options=None,
        **kwargs,
    )
    c.open_session(session_configuration=None, catalog=None, schema=None)

    assert captured.get("complex_types_as_json") is expected_flag


def test_execute_command_forwards_parameters_to_bind_param():
    """``execute_command(parameters=[...])`` routes each parameter
    through ``bind_tspark_params`` onto the kernel statement before
    ``execute()`` is called. Replaces the prior ``NotSupportedError``
    rejection now that the kernel-side ``Statement.bind_param`` is
    live (kernel PR #18)."""
    from databricks.sql.thrift_api.TCLIService import ttypes

    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    # Stub the statement chain so we can observe bind_param calls
    # without exercising the full ExecutedStatement → arrow_schema()
    # path (that's covered elsewhere).
    stmt = MagicMock()
    stmt.bind_param = MagicMock()
    stmt.execute.return_value = MagicMock(
        statement_id="stmt-id",
        arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
    )
    c._kernel_session.statement.return_value = stmt

    p1 = ttypes.TSparkParameter(ordinal=True, name=None, type="INT")
    p1.value = ttypes.TSparkParameterValue(stringValue="42")
    p2 = ttypes.TSparkParameter(ordinal=True, name=None, type="STRING")
    p2.value = ttypes.TSparkParameterValue(stringValue="hello")

    c.execute_command(
        operation="SELECT ?, ?",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[p1, p2],
        async_op=False,
        enforce_embedded_schema_correctness=False,
    )

    # bind_param was called once per TSparkParameter, in order, with
    # 1-based ordinals.
    assert stmt.bind_param.call_args_list == [
        ((1, "42", "INT"), {}),
        ((2, "hello", "STRING"), {}),
    ]
    # …and execute fired after binding.
    assert stmt.execute.called


def test_execute_command_forwards_query_tags():
    """Statement-level query_tags are forwarded to the kernel statement
    via set_query_tags (the kernel serialises them into the SEA
    query_tags conf). Previously rejected with NotSupportedError; now
    wired (kernel PR adding Statement.set_query_tags)."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    stmt = MagicMock()
    stmt.set_sql = MagicMock()
    stmt.set_query_tags = MagicMock()
    stmt.execute.return_value = MagicMock(
        statement_id="stmt-id",
        arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
    )
    c._kernel_session.statement.return_value = stmt

    tags = {"team": "platform", "production": None}
    c.execute_command(
        operation="SELECT 1",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
        query_tags=tags,
    )

    stmt.set_query_tags.assert_called_once_with(tags)
    assert stmt.execute.called


# ---------------------------------------------------------------------------
# Staging / volume operations — fail loud (not silently no-op)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "operation",
    [
        "PUT '/local/f.csv' INTO '/Volumes/c/s/v/f.csv'",
        "  put '/local/f' into '/Volumes/...'",  # leading ws + lowercase
        "GET '/Volumes/c/s/v/f' TO '/local/f'",
        "REMOVE '/Volumes/c/s/v/f'",
    ],
)
def test_staging_operation_raises_not_supported(operation):
    """Volume/staging PUT/GET/REMOVE must FAIL LOUD on the kernel path
    (the kernel can't perform the presigned-URL transfer; silently
    no-opping would make ETL ingest stale/missing data)."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    with pytest.raises(NotSupportedError, match="staging"):
        c.execute_command(
            operation=operation,
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )


@pytest.mark.parametrize(
    "operation, is_staging",
    [
        ("PUT '/f' INTO '/v'", True),
        ("get '/v' to '/f'", True),
        ("REMOVE '/v'", True),
        # Comment-prefixed staging ops MUST still be caught — otherwise
        # they slip into the silent-no-op bug this guard exists to close
        # (regression: review #1 on PR #825). ETL scripts commonly
        # prefix statements with comments.
        ("-- upload the file\nPUT '/f' INTO '/v'", True),
        ("/* staging */ PUT '/f' INTO '/v'", True),
        ("/* c1 */\n  -- c2\n  get '/v' to '/f'", True),  # mixed, multiple
        ("   \n\t PUT '/f' INTO '/v'", True),  # leading whitespace only
        ("SELECT 'GET' AS x", False),  # word appears but not leading verb
        ("SELECT * FROM puts", False),
        ("-- PUT in a comment\nSELECT 1", False),  # verb only in comment
        ("/* PUT */ SELECT 1", False),
        ("INSERT INTO t VALUES (1)", False),
        ("", False),
        ("-- just a comment", False),  # comment only, no statement
    ],
)
def test_is_staging_statement(operation, is_staging):
    assert kernel_client._is_staging_statement(operation) is is_staging


# ---------------------------------------------------------------------------
# Sync cancel wiring (cursor.cancel() during a blocking execute())
# ---------------------------------------------------------------------------


def test_cancel_running_cursor_fires_registered_canceller():
    """A canceller registered for a cursor (as execute_command does
    before the blocking call) is fired by cancel_running_cursor, which
    returns True."""
    c = _make_client()
    cursor = MagicMock()
    canceller = MagicMock()
    with c._sync_cancellers_lock:
        c._sync_cancellers[id(cursor)] = canceller

    assert c.cancel_running_cursor(cursor) is True
    canceller.cancel.assert_called_once_with()


def test_cancel_running_cursor_returns_false_when_none_registered():
    """No in-flight sync statement for this cursor -> False so the
    Cursor can emit its 'no executing command' warning."""
    c = _make_client()
    assert c.cancel_running_cursor(MagicMock()) is False


def test_cancel_running_cursor_swallows_cancel_errors():
    """cursor.cancel() is best-effort (PEP-249); a failing canceller
    (e.g. an early cancel before the statement id is observed, or a
    transport hiccup on the cancel RPC) must NOT propagate out of
    cancel(). It's swallowed+logged, and we still return True so the
    Cursor doesn't emit the misleading 'no executing command' warning
    (regression: review #2 on PR #825)."""
    c = _make_client()
    cursor = MagicMock()
    canceller = MagicMock()
    canceller.cancel.side_effect = RuntimeError("cancel RPC failed")
    with c._sync_cancellers_lock:
        c._sync_cancellers[id(cursor)] = canceller

    # Does not raise, returns True (a canceller was present + attempted).
    assert c.cancel_running_cursor(cursor) is True
    canceller.cancel.assert_called_once_with()


def test_execute_command_registers_and_clears_sync_canceller():
    """The sync execute() path registers a StatementCanceller keyed by
    the cursor before blocking, and clears it in the finally — so a
    concurrent cancel can reach it mid-flight, and it doesn't leak."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    canceller = MagicMock()
    stmt = MagicMock()
    stmt.canceller.return_value = canceller
    seen_during_execute = {}

    def fake_execute():
        # The canceller is registered *during* the blocking execute.
        with c._sync_cancellers_lock:
            seen_during_execute["registered"] = (
                c._sync_cancellers.get(id(cursor)) is canceller
            )
        return MagicMock(
            statement_id="stmt-id",
            arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
        )

    stmt.execute.side_effect = fake_execute
    c._kernel_session.statement.return_value = stmt

    c.execute_command(
        operation="SELECT 1",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
    )

    assert seen_during_execute["registered"] is True
    # Cleared after execute returns — no leak.
    with c._sync_cancellers_lock:
        assert id(cursor) not in c._sync_cancellers


def test_sync_execute_does_not_close_statement_on_success():
    """On a successful sync execute(), the connector must NOT close the
    parent kernel Statement — the kernel now auto-closes the server
    statement when the result stream drains (with the executed handle's
    Drop as backstop). A premature close() here broke lazy CloudFetch
    chunk-link fetches for large paginated-link results."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    stmt = MagicMock()
    stmt.execute.return_value = MagicMock(
        statement_id="stmt-id",
        arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
    )
    c._kernel_session.statement.return_value = stmt

    c.execute_command(
        operation="SELECT 1",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
    )

    # The kernel owns the statement lifecycle post-execute; connector
    # leaves it alone (kernel auto-close-on-drain + Drop backstop).
    stmt.close.assert_not_called()


def test_sync_execute_closes_statement_on_failure():
    """On the error path (execute raised, no executed handle / result
    set produced), the connector still closes the parent Statement so
    it isn't leaked."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    stmt = MagicMock()
    stmt.execute.side_effect = RuntimeError("boom")
    c._kernel_session.statement.return_value = stmt

    with pytest.raises(Exception):
        c.execute_command(
            operation="SELECT 1",
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )

    stmt.close.assert_called_once_with()


def test_get_columns_accepts_none_catalog():
    """The kernel's `list_columns` honours `catalog=None` by issuing
    `SHOW COLUMNS IN ALL CATALOGS` server-side. The connector should
    pass `None` through rather than rejecting it, matching the Thrift
    backend's `getColumns(null, …)` behaviour."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cursor.connection = MagicMock()
    # `list_columns` returns a stream the result-set wrapper will try
    # to call `arrow_schema()` on; give it a minimal fake.
    fake_stream = MagicMock()
    fake_stream.arrow_schema.return_value = MagicMock(__iter__=lambda self: iter([]))
    c._kernel_session.metadata.return_value.list_columns.return_value = fake_stream

    c.get_columns(
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        cursor=cursor,
        catalog_name=None,
    )
    # `list_columns` should be called with catalog=None, not rejected.
    c._kernel_session.metadata.return_value.list_columns.assert_called_once_with(
        catalog=None,
        schema_pattern=None,
        table_pattern=None,
        column_pattern=None,
    )


# ---------------------------------------------------------------------------
# Async handle bookkeeping
# ---------------------------------------------------------------------------


def test_cancel_command_tolerant_when_handle_missing():
    """``cancel_command`` is documented to be a no-op when there's
    no tracked async handle (matches Thrift's tolerance)."""
    c = _make_client()
    fake_command_id = CommandId.from_sea_statement_id("not-tracked")
    c.cancel_command(fake_command_id)  # must not raise


def test_close_command_tolerant_when_handle_missing():
    c = _make_client()
    fake_command_id = CommandId.from_sea_statement_id("not-tracked")
    c.close_command(fake_command_id)  # must not raise


def _attach_returns(c, *, status=None, status_error=None, await_result=None):
    """Wire ``_kernel_session.attach_async_statement`` to return a fake
    handle. ``status`` is a ``(state, failure)`` tuple for ``handle.status()``;
    ``status_error`` makes ``attach`` itself raise (e.g. NotFound);
    ``await_result`` sets ``handle.await_result()`` return value."""
    c._kernel_session = MagicMock()
    if status_error is not None:
        c._kernel_session.attach_async_statement.side_effect = status_error
        return None
    handle = MagicMock()
    if status is not None:
        handle.status.return_value = status
    if await_result is not None:
        handle.await_result.return_value = await_result
    c._kernel_session.attach_async_statement.return_value = handle
    return handle


def test_get_query_state_returns_succeeded_when_server_404s():
    """An id the server doesn't recognise (sync command whose id was
    never a standalone server statement, or an async command closed and
    aged out of the result TTL) surfaces as a NotFound KernelError from
    ``attach``; the client maps that to SUCCEEDED so the cursor's
    polling loop terminates cleanly."""
    c = _make_client()
    _attach_returns(c, status_error=_FakeKernelError(code="NotFound"))
    cid = CommandId.from_sea_statement_id("sync-only")
    assert c.get_query_state(cid) == CommandState.SUCCEEDED


def test_get_query_state_returns_closed_from_server():
    """A closed-but-not-yet-GC'd async command: the server still answers
    GetStatementStatus with state=CLOSED (200), which flows through the
    state map to ``CommandState.CLOSED`` — no connector-side
    closed-state bookkeeping."""
    c = _make_client()
    _attach_returns(c, status=("Closed", None))
    cid = CommandId.from_sea_statement_id("closed-async")
    assert c.get_query_state(cid) == CommandState.CLOSED


def test_get_query_state_propagates_non_not_found_error():
    """A transient/other error from ``attach`` (NOT NotFound) must not be
    silently swallowed as a terminal state — it propagates as a mapped
    PEP 249 exception so the caller can retry / surface it."""
    c = _make_client()
    _attach_returns(c, status_error=_FakeKernelError(code="Unavailable"))
    cid = CommandId.from_sea_statement_id("flaky")
    with pytest.raises(DatabaseError):
        c.get_query_state(cid)


def test_get_execution_result_attaches_by_id():
    """``get_execution_result`` re-attaches to the statement by id and
    awaits its result — no connector-side handle lookup."""
    c = _make_client()
    fake_stream = MagicMock()
    fake_stream.arrow_schema.return_value = pa.schema([("n", pa.int64())])
    handle = _attach_returns(c, await_result=fake_stream)
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cid = CommandId.from_sea_statement_id("async-1")

    rs = c.get_execution_result(cid, cursor=cursor)

    assert rs is not None
    c._kernel_session.attach_async_statement.assert_called_with("async-1")
    handle.await_result.assert_called_once_with()


def test_get_execution_result_maps_not_found_to_programming_error():
    """An unknown / aged-out id surfaces the kernel's NotFound as a
    mapped PEP 249 exception rather than a raw error."""
    c = _make_client()
    _attach_returns(c, status_error=_FakeKernelError(code="NotFound"))
    cid = CommandId.from_sea_statement_id("gone")
    with pytest.raises(DatabaseError):
        c.get_execution_result(cid, cursor=MagicMock())


def test_cancel_command_reraises_kernel_error():
    c = _make_client()
    fake_handle = MagicMock()
    fake_handle.cancel.side_effect = _FakeKernelError(code="Unavailable")
    cid = CommandId.from_sea_statement_id("abc")
    c._async_handles[cid.guid] = fake_handle
    with pytest.raises(OperationalError):
        c.cancel_command(cid)


def test_close_command_reraises_kernel_error():
    c = _make_client()
    fake_handle = MagicMock()
    fake_handle.close.side_effect = _FakeKernelError(code="Internal")
    cid = CommandId.from_sea_statement_id("abc")
    c._async_handles[cid.guid] = fake_handle
    with pytest.raises(DatabaseError):
        c.close_command(cid)
    # The handle is popped before the kernel call, so a subsequent
    # close_command is tolerantly a no-op.
    c.close_command(cid)


def test_get_query_state_raises_on_failed_state_with_failure():
    c = _make_client()
    _attach_returns(
        c, status=("Failed", _FakeKernelError(code="SqlError", message="bad"))
    )
    cid = CommandId.from_sea_statement_id("abc")
    with pytest.raises(DatabaseError, match="bad"):
        c.get_query_state(cid)


def test_get_query_state_handles_non_baseexception_failure():
    """If the kernel's status() ever returns a ``failure`` that isn't
    a real ``KernelError`` (struct, dict, custom type — kernel API
    drift), ``get_query_state`` must still surface a mapped PEP 249
    exception. The naive ``raise ... from failure`` would raise
    ``TypeError: exception causes must derive from BaseException``;
    the wrap helper deals with it."""
    c = _make_client()
    # ``failure`` is a plain dict (not BaseException) — simulates a
    # kernel binding that exposes the failure as a structured value.
    _attach_returns(c, status=("Failed", {"code": "Internal", "msg": "weird"}))
    cid = CommandId.from_sea_statement_id("xyz")
    # Must surface as a PEP 249 exception (OperationalError via the
    # wrap helper's fallback path), not TypeError.
    with pytest.raises(OperationalError):
        c.get_query_state(cid)


def test_get_query_state_returns_state_when_no_failure():
    c = _make_client()
    _attach_returns(c, status=("Running", None))
    cid = CommandId.from_sea_statement_id("abc")
    assert c.get_query_state(cid) == CommandState.RUNNING


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_max_download_threads_is_nonzero():
    """Property is consulted by Thrift code paths that don't run for
    ``use_kernel=True``; a non-zero default avoids divide-by-zero."""
    c = _make_client()
    assert c.max_download_threads > 0


def test_synthetic_command_id_is_uuid_shaped():
    """Synthetic metadata command IDs are plain hex UUIDs (no
    ``metadata-`` prefix) so anything reading ``cursor.query_id``
    downstream sees a parseable shape."""
    c = _make_client()
    cid = c._synthetic_command_id()
    # 32-char lowercase hex
    assert len(cid.guid) == 32
    int(cid.guid, 16)  # raises if non-hex


def test_close_session_clears_async_handles_even_if_close_fails():
    """Per-handle close errors are logged but don't prevent the
    rest of the close-session sweep from completing, and the dict
    is cleared either way."""
    c = _make_client()
    good = MagicMock()
    bad = MagicMock()
    bad.close.side_effect = _FakeKernelError(code="Unavailable")
    c._async_handles["a"] = good
    c._async_handles["b"] = bad
    c._kernel_session = MagicMock()
    c.close_session(MagicMock())
    assert c._async_handles == {}
    assert good.close.called
    assert bad.close.called


def test_close_session_closes_and_drops_swept_handles():
    """Close-session closes every tracked async handle (firing its
    server-side CloseStatement) and drops it from the keep-alive map.
    There is no connector-side closed-state bookkeeping — a subsequent
    ``get_query_state`` re-attaches by id and reads CLOSED from the
    server."""
    c = _make_client()
    handle = MagicMock()
    cid = CommandId.from_sea_statement_id("xyz")
    c._async_handles[cid.guid] = handle
    c._kernel_session = MagicMock()
    c.close_session(MagicMock())
    assert handle.close.called
    assert c._async_handles == {}


# ---------------------------------------------------------------------------
# CLOSED command-state comes from the server (re-attach by id)
# ---------------------------------------------------------------------------


def test_get_query_state_returns_closed_after_close_command():
    """After ``close_command`` fires the server-side CloseStatement, a
    subsequent ``get_query_state`` re-attaches by id and the server
    reports CLOSED (200 state=CLOSED until the result TTL elapses). No
    connector-side closed-state tracking — the server is the source of
    truth."""
    c = _make_client()
    c._kernel_session = MagicMock()
    handle = MagicMock()
    cid = CommandId.from_sea_statement_id("async-1")
    c._async_handles[cid.guid] = handle
    c.close_command(cid)
    assert handle.close.called

    # Re-attach now reports CLOSED from the server.
    attached = MagicMock()
    attached.status.return_value = ("Closed", None)
    c._kernel_session.attach_async_statement.return_value = attached
    assert c.get_query_state(cid) == CommandState.CLOSED


# ---------------------------------------------------------------------------
# PyO3 native exceptions (M2) — non-KernelError wrapping
# ---------------------------------------------------------------------------


def test_pyo3_native_exception_wrapped_as_operational_error():
    """A PyO3 boundary error that is *not* a ``KernelError`` (e.g.
    ``TypeError`` from argument conversion) must surface as a PEP
    249 exception, not propagate raw to connector callers."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    # Statement chain succeeds, but ``execute`` raises a raw
    # ``TypeError`` (simulating PyO3 argument-conversion failure).
    stmt = MagicMock()
    stmt.execute.side_effect = TypeError("argument 'foo' must be str, not int")
    c._kernel_session.statement.return_value = stmt
    with pytest.raises(
        OperationalError, match="Unexpected error from databricks_sql_kernel"
    ):
        c.execute_command(
            operation="SELECT 1",
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )


def test_pyo3_native_exception_wrapped_for_metadata_calls():
    """Same wrapping for every metadata method."""
    c = _make_client()
    c._kernel_session = MagicMock()
    md = c._kernel_session.metadata.return_value
    md.list_catalogs.side_effect = ValueError("bad PyO3 arg")
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    with pytest.raises(OperationalError):
        c.get_catalogs(session_id=MagicMock(), max_rows=1, max_bytes=1, cursor=cursor)


# ---------------------------------------------------------------------------
# Schema-on-construct race (M3) — KernelError during arrow_schema()
# ---------------------------------------------------------------------------


def test_kernel_error_during_result_set_construction_is_mapped():
    """``KernelResultSet.__init__`` calls
    ``kernel_handle.arrow_schema()`` which can itself raise a
    ``KernelError``. The connector must catch that and surface a
    mapped PEP 249 exception, not let the raw ``KernelError``
    escape."""
    c = _make_client()
    c._kernel_session = MagicMock()
    md = c._kernel_session.metadata.return_value
    bad_stream = MagicMock()
    bad_stream.arrow_schema.side_effect = _FakeKernelError(
        code="SqlError", message="schema unavailable"
    )
    md.list_catalogs.return_value = bad_stream
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    with pytest.raises(DatabaseError, match="schema unavailable"):
        c.get_catalogs(session_id=MagicMock(), max_rows=1, max_bytes=1, cursor=cursor)


# ---------------------------------------------------------------------------
# get_execution_result is re-callable via attach-by-id
# ---------------------------------------------------------------------------


def test_get_execution_result_is_re_callable():
    """``get_execution_result`` re-attaches by id on every call, so a
    second fetch for the same async command succeeds (Thrift-parity
    re-fetch). Each call attaches a fresh handle and awaits its result;
    neither raises, and the connector never depended on a retained
    handle. The kernel's ``await_result()`` is idempotent server-side."""
    c = _make_client()
    c._kernel_session = MagicMock()
    fake_stream = MagicMock()
    fake_stream.arrow_schema.return_value = pa.schema([("n", pa.int64())])
    handle = MagicMock()
    handle.await_result.return_value = fake_stream
    c._kernel_session.attach_async_statement.return_value = handle
    cid = CommandId.from_sea_statement_id("async-recall-twice")
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    rs1 = c.get_execution_result(cid, cursor=cursor)
    rs2 = c.get_execution_result(cid, cursor=cursor)

    assert rs1 is not None and rs2 is not None
    # Two calls -> two attaches -> two await_results. No reliance on a
    # connector-tracked handle.
    assert c._kernel_session.attach_async_statement.call_count == 2
    assert handle.await_result.call_count == 2


# ---------------------------------------------------------------------------
# get_tables — table_types is filtered kernel-side (no connector drain)
# ---------------------------------------------------------------------------


def _make_tables_stream() -> MagicMock:
    """Build a fake stream that mimics the kernel's ``list_tables``
    output shape. The kernel applies the ``table_types`` filter
    itself, so the connector now forwards ``table_types`` and returns
    this stream unchanged — these tests mock the kernel filter away
    and only assert the forwarded args + pass-through behaviour."""
    schema = pa.schema(
        [
            ("TABLE_CAT", pa.string()),
            ("TABLE_SCHEM", pa.string()),
            ("TABLE_NAME", pa.string()),
            ("EXTRA_1", pa.string()),
            ("EXTRA_2", pa.string()),
            ("TABLE_TYPE", pa.string()),
        ]
    )
    table = pa.table(
        {
            "TABLE_CAT": ["main", "main", "main"],
            "TABLE_SCHEM": ["s", "s", "s"],
            "TABLE_NAME": ["t1", "t2", "v1"],
            "EXTRA_1": ["", "", ""],
            "EXTRA_2": ["", "", ""],
            "TABLE_TYPE": ["TABLE", "TABLE", "VIEW"],
        },
        schema=schema,
    )
    batches = table.to_batches()
    stream = MagicMock()
    stream.arrow_schema.return_value = schema
    # First call returns the batch; second returns None (exhausted).
    stream.fetch_next_batch.side_effect = batches + [None]
    return stream


def test_get_tables_forwards_table_types_to_kernel():
    """``table_types`` is forwarded verbatim to the kernel's
    ``list_tables`` (which filters case-insensitively) — the
    connector no longer drains + refilters client-side. The stream
    flows back through the normal ``KernelResultSet`` path unchanged."""
    c = _make_client()
    c._kernel_session = MagicMock()
    list_tables = c._kernel_session.metadata.return_value.list_tables
    list_tables.return_value = _make_tables_stream()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    rs = c.get_tables(
        session_id=MagicMock(),
        max_rows=10,
        max_bytes=1,
        cursor=cursor,
        table_types=["view"],
    )

    list_tables.assert_called_once_with(
        catalog=None,
        schema_pattern=None,
        table_pattern=None,
        table_types=["view"],
    )
    # Stream is returned as-is — no connector-side row filtering. The
    # mock kernel doesn't filter, so all three rows pass through.
    table = rs.fetchall_arrow()
    assert table.num_rows == 3


def test_get_tables_without_table_types_passes_none():
    """No filter → ``table_types=None`` forwarded; stream flows
    through unchanged via the normal ``KernelResultSet`` path."""
    c = _make_client()
    c._kernel_session = MagicMock()
    list_tables = c._kernel_session.metadata.return_value.list_tables
    list_tables.return_value = _make_tables_stream()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    rs = c.get_tables(
        session_id=MagicMock(),
        max_rows=10,
        max_bytes=1,
        cursor=cursor,
        table_types=None,
    )

    list_tables.assert_called_once_with(
        catalog=None,
        schema_pattern=None,
        table_pattern=None,
        table_types=None,
    )
    table = rs.fetchall_arrow()
    assert table.num_rows == 3


# ---------------------------------------------------------------------------
# Cursor-state tracking (T7) — active_command_id consistency
# ---------------------------------------------------------------------------


def _stream_with_schema() -> MagicMock:
    """A minimal fake kernel handle whose ``arrow_schema()`` returns a
    real schema so ``KernelResultSet.__init__`` succeeds."""
    stream = MagicMock()
    stream.arrow_schema.return_value = pa.schema([("x", pa.int64())])
    return stream


def test_metadata_call_sets_active_command_id():
    """Metadata methods mint a synthetic command id and must publish
    it on the cursor (Thrift sets ``active_command_id`` unconditionally
    in ``_handle_execute_response``). Without this, ``cursor.query_id``
    would stay pinned to a prior query after a metadata browse."""
    c = _make_client()
    c._kernel_session = MagicMock()
    c._kernel_session.metadata.return_value.list_catalogs.return_value = (
        _stream_with_schema()
    )
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cursor.active_command_id = None

    c.get_catalogs(session_id=MagicMock(), max_rows=1, max_bytes=1, cursor=cursor)

    assert cursor.active_command_id is not None
    # The synthetic id is a UUID-shaped guid the cursor can attribute
    # logs to (rather than a stale prior-query id).
    assert cursor.active_command_id.guid


def test_sync_execute_sets_active_command_id():
    """A successful sync execute publishes the server statement id on
    the cursor."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cursor.active_command_id = None

    stmt = MagicMock()
    stmt.execute.return_value = MagicMock(
        statement_id="stmt-abc",
        num_modified_rows=None,
        arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
    )
    c._kernel_session.statement.return_value = stmt

    c.execute_command(
        operation="SELECT 1",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
    )

    assert cursor.active_command_id is not None
    assert cursor.active_command_id.guid == "stmt-abc"


def test_failed_sync_execute_sets_active_command_id_from_canceller():
    """When execute() fails *after* the server assigned a statement id,
    the canceller's inflight slot holds that id. The connector reads it
    and publishes ``active_command_id`` before re-raising, so the cursor
    can correlate the failed query (telemetry / log lookup)."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cursor.active_command_id = None

    canceller = MagicMock()
    canceller.statement_id.return_value = "failed-stmt-id"
    stmt = MagicMock()
    stmt.canceller.return_value = canceller
    stmt.execute.side_effect = RuntimeError("boom after id assigned")
    c._kernel_session.statement.return_value = stmt

    with pytest.raises(Exception):
        c.execute_command(
            operation="SELECT 1",
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )

    assert cursor.active_command_id is not None
    assert cursor.active_command_id.guid == "failed-stmt-id"


def test_failed_sync_execute_leaves_active_command_id_untouched_when_no_id():
    """A pre-id transport failure (canceller has no statement id yet)
    must leave ``active_command_id`` untouched — there's nothing to
    correlate, and clobbering it would lie about cursor state."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    sentinel = object()
    cursor.active_command_id = sentinel

    canceller = MagicMock()
    canceller.statement_id.return_value = None  # no id observed yet
    stmt = MagicMock()
    stmt.canceller.return_value = canceller
    stmt.execute.side_effect = RuntimeError("connect refused")
    c._kernel_session.statement.return_value = stmt

    with pytest.raises(Exception):
        c.execute_command(
            operation="SELECT 1",
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[],
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )

    assert cursor.active_command_id is sentinel


def test_sync_execute_forwards_num_modified_rows_to_rowcount():
    """DML reports a real ``cursor.rowcount`` from the kernel's
    ``num_modified_rows`` instead of the hardcoded ``-1``."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cursor.rowcount = -1

    stmt = MagicMock()
    stmt.execute.return_value = MagicMock(
        statement_id="dml-1",
        num_modified_rows=42,
        arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
    )
    c._kernel_session.statement.return_value = stmt

    c.execute_command(
        operation="INSERT INTO t VALUES (1)",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
    )

    assert cursor.rowcount == 42


def test_sync_execute_leaves_rowcount_default_when_num_modified_rows_none():
    """SELECT (and warehouses that don't report it) → ``None`` leaves
    ``rowcount`` at its ``-1`` default."""
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    cursor.rowcount = -1

    stmt = MagicMock()
    stmt.execute.return_value = MagicMock(
        statement_id="select-1",
        num_modified_rows=None,
        arrow_schema=MagicMock(return_value=pa.schema([("x", pa.int64())])),
    )
    c._kernel_session.statement.return_value = stmt

    c.execute_command(
        operation="SELECT 1",
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        lz4_compression=False,
        cursor=cursor,
        use_cloud_fetch=False,
        parameters=[],
        async_op=False,
        enforce_embedded_schema_correctness=False,
    )

    assert cursor.rowcount == -1


# ---------------------------------------------------------------------------
# Metadata filter normalization — wildcard catalog + empty-string patterns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("wildcard", ["%", "*", "", "   "])
def test_get_columns_normalizes_wildcard_catalog_to_none(wildcard):
    """``catalog_name`` of ``%``/``*``/blank → ``None`` (all-catalogs),
    matching JDBC exact-or-all semantics and keeping the three metadata
    methods symmetric."""
    c = _make_client()
    c._kernel_session = MagicMock()
    list_columns = c._kernel_session.metadata.return_value.list_columns
    list_columns.return_value = _stream_with_schema()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    c.get_columns(
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        cursor=cursor,
        catalog_name=wildcard,
        schema_name="s",
        table_name="t",
        column_name="c",
    )

    list_columns.assert_called_once_with(
        catalog=None,
        schema_pattern="s",
        table_pattern="t",
        column_pattern="c",
    )


def test_get_schemas_normalizes_blank_pattern_to_none():
    """An empty-string schema pattern → ``None`` (match-all), mapping
    the kernel's ``InvalidArgument``-on-``""`` to Thrift's effective
    match-all. ``%``/``*`` stay as real LIKE wildcards on patterns."""
    c = _make_client()
    c._kernel_session = MagicMock()
    list_schemas = c._kernel_session.metadata.return_value.list_schemas
    list_schemas.return_value = _stream_with_schema()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    c.get_schemas(
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        cursor=cursor,
        catalog_name="main",
        schema_name="",
    )

    list_schemas.assert_called_once_with(catalog="main", schema_pattern=None)


def test_get_schemas_keeps_wildcard_pattern():
    """A ``%`` schema pattern is a real LIKE wildcard — passed through,
    NOT normalized to None."""
    c = _make_client()
    c._kernel_session = MagicMock()
    list_schemas = c._kernel_session.metadata.return_value.list_schemas
    list_schemas.return_value = _stream_with_schema()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024

    c.get_schemas(
        session_id=MagicMock(),
        max_rows=1,
        max_bytes=1,
        cursor=cursor,
        catalog_name="main",
        schema_name="prod_%",
    )

    list_schemas.assert_called_once_with(catalog="main", schema_pattern="prod_%")


# ---------------------------------------------------------------------------
# TLS translation: SSLOptions -> kernel Session tls_* kwargs.
# ---------------------------------------------------------------------------


class TestKernelTlsKwargs:
    """``_kernel_tls_kwargs`` maps the connector's ``SSLOptions`` onto
    the kernel ``Session`` ``tls_*`` kwargs, reading cert files into
    PEM bytes and inverting the verify→skip booleans."""

    def _ssl_options(self, **overrides):
        from databricks.sql.types import SSLOptions

        return SSLOptions(**overrides)

    def test_default_ssl_options_emit_no_tls_kwargs(self):
        # Stock TLS (verify on, no custom CA) → kernel keeps its secure
        # default, so we emit nothing.
        assert kernel_client._kernel_tls_kwargs(self._ssl_options()) == {}

    def test_none_ssl_options_emit_no_tls_kwargs(self):
        assert kernel_client._kernel_tls_kwargs(None) == {}

    def test_verify_off_maps_to_skip_verify_and_hostname(self):
        # tls_verify=False disables all chain validation, which subsumes
        # hostname verification — so both skip flags are emitted, matching
        # SSLOptions.create_ssl_context (check_hostname=False when
        # tls_verify is False).
        out = kernel_client._kernel_tls_kwargs(self._ssl_options(tls_verify=False))
        assert out == {
            "tls_skip_verify": True,
            "tls_skip_hostname_verify": True,
        }

    def test_hostname_verify_off_maps_to_skip_hostname(self):
        # Only hostname verification off (chain still validated) → just
        # the hostname skip, no tls_skip_verify.
        out = kernel_client._kernel_tls_kwargs(
            self._ssl_options(tls_verify_hostname=False)
        )
        assert out == {"tls_skip_hostname_verify": True}

    def test_custom_ca_file_read_as_bytes(self, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_bytes(b"-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n")
        out = kernel_client._kernel_tls_kwargs(
            self._ssl_options(tls_trusted_ca_file=str(ca))
        )
        assert out["tls_ca_cert"] == ca.read_bytes()

    def test_mtls_cert_and_key_read_as_bytes(self, tmp_path):
        cert = tmp_path / "client.crt"
        key = tmp_path / "client.key"
        cert.write_bytes(b"CERTBYTES")
        key.write_bytes(b"KEYBYTES")
        out = kernel_client._kernel_tls_kwargs(
            self._ssl_options(
                tls_client_cert_file=str(cert),
                tls_client_cert_key_file=str(key),
            )
        )
        assert out["tls_client_cert"] == b"CERTBYTES"
        assert out["tls_client_key"] == b"KEYBYTES"

    def test_mtls_cert_only_falls_back_to_cert_for_key(self, tmp_path):
        # SSLOptions allows a combined cert+key file (key_file None);
        # the kernel needs both, so we reuse the cert file for the key.
        combined = tmp_path / "combined.pem"
        combined.write_bytes(b"COMBINED")
        out = kernel_client._kernel_tls_kwargs(
            self._ssl_options(tls_client_cert_file=str(combined))
        )
        assert out["tls_client_cert"] == b"COMBINED"
        assert out["tls_client_key"] == b"COMBINED"

    def test_encrypted_client_key_rejected(self, tmp_path):
        cert = tmp_path / "client.crt"
        cert.write_bytes(b"CERT")
        with pytest.raises(NotSupportedError, match="password-protected"):
            kernel_client._kernel_tls_kwargs(
                self._ssl_options(
                    tls_client_cert_file=str(cert),
                    tls_client_cert_key_password="hunter2",
                )
            )

    def test_missing_ca_file_raises_programming_error(self):
        with pytest.raises(ProgrammingError, match="tls_trusted_ca_file"):
            kernel_client._kernel_tls_kwargs(
                self._ssl_options(tls_trusted_ca_file="/no/such/ca.pem")
            )


# ---------------------------------------------------------------------------
# Secret hygiene: oauth_client_secret must not be retained or leak.
# ---------------------------------------------------------------------------


class TestKernelSecretHygiene:
    """The new ``oauth_client_secret`` M2M kwarg is credential material.
    Verify it's scrubbed after ``open_session`` and never exposed via
    the connector's ``repr`` / ``vars`` or through a mapped exception."""

    _SECRET = "super-secret-m2m-value"

    def _client_with_m2m(self, monkeypatch, captured):
        def fake_session(**kw):
            captured.update(kw)
            sess = MagicMock()
            sess.session_id = "sess-id"
            return sess

        monkeypatch.setattr(kernel_client._kernel, "Session", fake_session)
        return kernel_client.KernelDatabricksClient(
            server_hostname="example.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_provider=AccessTokenAuthProvider("dapi-test"),
            ssl_options=None,
            auth_options={
                "oauth_client_id": "sp-uuid",
                "oauth_client_secret": self._SECRET,
            },
        )

    def test_secret_forwarded_then_scrubbed_from_auth_options(self, monkeypatch):
        captured = {}
        c = self._client_with_m2m(monkeypatch, captured)
        c.open_session(session_configuration=None, catalog=None, schema=None)

        # Forwarded to the kernel Session as client_secret...
        assert captured.get("client_secret") == self._SECRET
        # ...but not retained on the long-lived connector.
        assert "oauth_client_secret" not in c._auth_options

    def test_secret_absent_from_repr_and_vars_after_open(self, monkeypatch):
        captured = {}
        c = self._client_with_m2m(monkeypatch, captured)
        c.open_session(session_configuration=None, catalog=None, schema=None)

        assert self._SECRET not in repr(c)
        assert self._SECRET not in str(vars(c))

    def test_secret_not_in_mapped_open_session_exception(self, monkeypatch):
        # If the kernel Session constructor raises with the secret in its
        # message/args, the mapped PEP-249 exception must still be raised
        # (we don't assert the kernel scrubs its own error — that's the
        # kernel's job — but we confirm the connector's scrub still runs
        # via the finally block even on the failure path).
        def boom_session(**kw):
            raise RuntimeError("kernel open failed")

        monkeypatch.setattr(kernel_client._kernel, "Session", boom_session)
        c = kernel_client.KernelDatabricksClient(
            server_hostname="example.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_provider=AccessTokenAuthProvider("dapi-test"),
            ssl_options=None,
            auth_options={
                "oauth_client_id": "sp-uuid",
                "oauth_client_secret": self._SECRET,
            },
        )
        with pytest.raises(Exception):
            c.open_session(session_configuration=None, catalog=None, schema=None)
        # Scrubbed even though open_session raised (finally block).
        assert "oauth_client_secret" not in c._auth_options


class TestKernelTlsEmptyCaFile:
    def test_empty_ca_file_raises_programming_error(self, tmp_path):
        from databricks.sql.types import SSLOptions

        ca = tmp_path / "empty.pem"
        ca.write_bytes(b"   \n")
        with pytest.raises(ProgrammingError, match="is empty"):
            kernel_client._kernel_tls_kwargs(SSLOptions(tls_trusted_ca_file=str(ca)))


# ---------------------------------------------------------------------------
# Retry translation: connector _retry_* -> kernel Session retry kwargs.
# ---------------------------------------------------------------------------


class TestKernelRetryKwargs:
    """``_kernel_retry_kwargs`` maps the connector's ``_retry_*`` tuning
    onto the kernel ``Session``'s ``retry_*`` kwargs, rounding float
    seconds to whole seconds and forwarding the total-attempts count
    1:1 (the kernel does the retries-after-first conversion)."""

    def test_empty_options_emit_no_kwargs(self):
        assert kernel_client._kernel_retry_kwargs({}) == {}

    def test_all_options_mapped(self):
        out = kernel_client._kernel_retry_kwargs(
            {
                "retry_delay_min": 2.0,
                "retry_delay_max": 90.0,
                "retry_stop_after_attempts_count": 10,
                "retry_stop_after_attempts_duration": 600.0,
            }
        )
        assert out == {
            "retry_min_wait_secs": 2,
            "retry_max_wait_secs": 90,
            "retry_max_attempts": 10,
            "retry_overall_timeout_secs": 600,
        }

    def test_count_forwarded_one_to_one(self):
        # Total-attempts count is passed verbatim; the kernel converts
        # to retries-after-first internally (so 1 means a single attempt).
        out = kernel_client._kernel_retry_kwargs({"retry_stop_after_attempts_count": 1})
        assert out == {"retry_max_attempts": 1}

    def test_float_seconds_rounded(self):
        out = kernel_client._kernel_retry_kwargs(
            {"retry_delay_min": 2.4, "retry_delay_max": 2.6}
        )
        assert out == {"retry_min_wait_secs": 2, "retry_max_wait_secs": 3}

    def test_subsecond_delay_floored_to_one(self):
        # A positive sub-second delay (the connector allows 0.1) must
        # not round down to 0 — that would turn backoff into busy-retry.
        out = kernel_client._kernel_retry_kwargs({"retry_delay_min": 0.1})
        assert out == {"retry_min_wait_secs": 1}

    def test_only_set_keys_emitted(self):
        out = kernel_client._kernel_retry_kwargs({"retry_delay_max": 30.0})
        assert out == {"retry_max_wait_secs": 30}

    def test_retry_delay_default_has_no_mapping(self):
        # _retry_delay_default isn't forwarded by session.py and isn't a
        # recognised key here — it has no kernel equivalent.
        out = kernel_client._kernel_retry_kwargs({"retry_delay_default": 5.0})
        assert out == {}


class TestKernelHttpHeadersForwarding:
    """http_headers (the connector's caller headers + composed
    User-Agent + SPOG org-id) are forwarded to the kernel Session as the
    ``http_headers`` kwarg. The kernel applies them per request (its own
    Authorization / org-id win; a caller User-Agent is appended to the
    kernel base UA)."""

    def _open_capturing(self, monkeypatch, http_headers):
        captured = {}

        def fake_session(**kw):
            captured.update(kw)
            sess = MagicMock()
            sess.session_id = "sess-id"
            return sess

        monkeypatch.setattr(kernel_client._kernel, "Session", fake_session)
        c = kernel_client.KernelDatabricksClient(
            server_hostname="example.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            auth_provider=AccessTokenAuthProvider("dapi-test"),
            ssl_options=None,
            http_headers=http_headers,
        )
        c.open_session(session_configuration=None, catalog=None, schema=None)
        return captured

    def test_http_headers_forwarded_to_kernel_session(self, monkeypatch):
        headers = [
            ("User-Agent", "PyDatabricksSqlConnector/4.0 (myentry)"),
            ("X-Custom", "v1"),
        ]
        captured = self._open_capturing(monkeypatch, headers)
        assert captured.get("http_headers") == [
            ("User-Agent", "PyDatabricksSqlConnector/4.0 (myentry)"),
            ("X-Custom", "v1"),
        ]

    def test_no_http_headers_omits_kwarg(self, monkeypatch):
        # Empty/none headers → the kwarg isn't passed at all (kernel
        # keeps its defaults).
        captured = self._open_capturing(monkeypatch, [])
        assert "http_headers" not in captured

    def test_authorization_and_org_id_dropped_before_forwarding(self, monkeypatch):
        # The connector must NOT forward Authorization / x-databricks-org-id
        # to the kernel — the kernel manages both (and warns per request
        # if it sees them). Double-walls the kernel's own skip.
        headers = [
            ("Authorization", "Bearer should-not-forward"),
            ("X-Databricks-Org-Id", "12345"),
            ("User-Agent", "PyDatabricksSqlConnector/4.0 (e)"),
            ("X-Keep", "yes"),
        ]
        captured = self._open_capturing(monkeypatch, headers)
        fwd = captured.get("http_headers")
        names = {n.lower() for n, _ in fwd}
        assert "authorization" not in names
        assert "x-databricks-org-id" not in names
        # Non-reserved headers (incl. User-Agent) still forwarded.
        assert ("User-Agent", "PyDatabricksSqlConnector/4.0 (e)") in fwd
        assert ("X-Keep", "yes") in fwd

    def test_only_reserved_headers_omits_kwarg(self, monkeypatch):
        # If the only headers are reserved ones, nothing is forwarded
        # and the kwarg is omitted entirely.
        captured = self._open_capturing(
            monkeypatch, [("Authorization", "Bearer x"), ("x-databricks-org-id", "1")]
        )
        assert "http_headers" not in captured
