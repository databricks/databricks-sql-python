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
        ("SqlError", DatabaseError),
        ("Unknown", DatabaseError),
    ],
)
def test_code_to_exception_mapping(code, expected_cls):
    """Every entry in ``_CODE_TO_EXCEPTION`` maps to the documented
    PEP 249 class."""
    err = _FakeKernelError(code=code, message=f"{code} boom")
    out = kernel_client._reraise_kernel_error(err)
    assert isinstance(out, expected_cls)
    assert "boom" in str(out)
    assert out.__cause__ is err


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


def test_open_session_rejects_double_open(monkeypatch):
    """Two ``open_session`` calls on the same client must fail —
    the kernel session is bound to a single open call."""
    c = _make_client()
    c._kernel_session = MagicMock()  # pretend already open
    with pytest.raises(InterfaceError, match="already has an open session"):
        c.open_session(session_configuration=None, catalog=None, schema=None)


def test_execute_command_rejects_parameters():
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    with pytest.raises(NotSupportedError, match="Parameter binding"):
        c.execute_command(
            operation="SELECT ?",
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            lz4_compression=False,
            cursor=cursor,
            use_cloud_fetch=False,
            parameters=[object()],  # any non-empty list
            async_op=False,
            enforce_embedded_schema_correctness=False,
        )


def test_execute_command_rejects_query_tags():
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    with pytest.raises(NotSupportedError, match="query_tags"):
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
            query_tags={"team": "x"},
        )


def test_get_columns_requires_catalog():
    c = _make_client()
    c._kernel_session = MagicMock()
    cursor = MagicMock()
    cursor.arraysize = 100
    cursor.buffer_size_bytes = 1024
    with pytest.raises(ProgrammingError, match="catalog_name"):
        c.get_columns(
            session_id=MagicMock(),
            max_rows=1,
            max_bytes=1,
            cursor=cursor,
            catalog_name=None,
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


def test_get_query_state_returns_succeeded_when_handle_missing():
    """Sync-execute paths never register an async handle; by the
    time ``get_query_state`` could be called the command is
    terminal-by-construction. The client returns SUCCEEDED so the
    cursor's polling loop terminates cleanly."""
    c = _make_client()
    fake_command_id = CommandId.from_sea_statement_id("sync-only")
    assert c.get_query_state(fake_command_id) == CommandState.SUCCEEDED


def test_get_execution_result_raises_for_unknown_command_id():
    """The kernel backend only tracks async-submitted statements;
    a ``get_execution_result`` call for an unknown id is a
    programming error."""
    c = _make_client()
    fake_command_id = CommandId.from_sea_statement_id("unknown")
    with pytest.raises(ProgrammingError, match="unknown command_id"):
        c.get_execution_result(fake_command_id, cursor=MagicMock())


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
    fake_handle = MagicMock()
    fake_handle.status.return_value = (
        "Failed",
        _FakeKernelError(code="SqlError", message="bad"),
    )
    cid = CommandId.from_sea_statement_id("abc")
    c._async_handles[cid.guid] = fake_handle
    with pytest.raises(DatabaseError, match="bad"):
        c.get_query_state(cid)


def test_get_query_state_returns_state_when_no_failure():
    c = _make_client()
    fake_handle = MagicMock()
    fake_handle.status.return_value = ("Running", None)
    cid = CommandId.from_sea_statement_id("abc")
    c._async_handles[cid.guid] = fake_handle
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
