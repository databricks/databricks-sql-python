"""``DatabricksClient`` backed by the Rust kernel via PyO3.

Routed when ``use_kernel=True``. Constructor takes the connector's
already-built ``auth_provider`` and forwards everything else to the
kernel's ``Session``. Every kernel call goes through this thin
wrapper; this module is the single seam between the connector's
``DatabricksClient`` contract and the kernel's Python surface.

Errors map cleanly: ``KernelError`` from the kernel is inspected
for its ``code`` attribute and re-raised as the appropriate PEP
249 exception (``DatabaseError``, ``OperationalError``,
``ProgrammingError``, etc.). Connector callers see standard
exception types, never the underlying kernel error.

Phase 1 gaps documented in the integration design:

- Parameter binding (``parameters=[TSparkParameter, ...]``) is not
  yet supported — the PyO3 ``Statement`` doesn't expose
  ``bind_param``. ``execute_command(parameters=[...])`` raises
  ``NotSupportedError``.
- ``query_tags`` on execute is not supported (kernel exposes
  ``statement_conf`` but PyO3 doesn't surface it).
- ``get_tables`` with a non-empty ``table_types`` filter applies
  the filter client-side; today the kernel returns the full
  ``SHOW TABLES`` shape unchanged. The connector's existing
  ``ResultSetFilter.filter_tables_by_type`` is keyed on
  ``SeaResultSet`` not ``KernelResultSet``, so we punt and let
  the caller see all rows — documented as a known gap in the
  design doc.
- Volume PUT/GET (staging operations): kernel has no Volume API
  yet. Users on Thrift-only paths.
"""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.kernel.auth_bridge import kernel_auth_kwargs
from databricks.sql.backend.kernel.result_set import KernelResultSet
from databricks.sql.backend.types import (
    BackendType,
    CommandId,
    CommandState,
    SessionId,
)
from databricks.sql.exc import (
    DatabaseError,
    Error,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from databricks.sql.thrift_api.TCLIService import ttypes

if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

logger = logging.getLogger(__name__)


try:
    import databricks_sql_kernel as _kernel  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover - import-time error surfaces clearly
    # The `databricks-sql-kernel` wheel is not yet on PyPI, so we
    # don't yet declare it as an optional extra in pyproject.toml
    # (doing so breaks `poetry lock`). Once published the install
    # hint will move to `pip install 'databricks-sql-connector[kernel]'`.
    raise ImportError(
        "use_kernel=True requires the databricks-sql-kernel package. Install it with:\n"
        "  pip install databricks-sql-kernel\n"
        "or for local development from the kernel repo:\n"
        "  cd databricks-sql-kernel/pyo3 && maturin develop --release"
    ) from exc


# ─── Error mapping ──────────────────────────────────────────────────────────


# Map a kernel `code` slug to the PEP 249 exception class that best
# captures it. The match isn't a perfect 1:1 — PEP 249 has a
# narrower taxonomy than the kernel — so several kernel codes
# collapse onto the same Python exception. This table is the only
# place that mapping lives.
_CODE_TO_EXCEPTION = {
    "InvalidArgument": ProgrammingError,
    "Unauthenticated": OperationalError,
    "PermissionDenied": OperationalError,
    "NotFound": ProgrammingError,
    "ResourceExhausted": OperationalError,
    "Unavailable": OperationalError,
    "Timeout": OperationalError,
    "Cancelled": OperationalError,
    "DataLoss": DatabaseError,
    "Internal": DatabaseError,
    "InvalidStatementHandle": ProgrammingError,
    "NetworkError": OperationalError,
    "SqlError": DatabaseError,
    "Unknown": DatabaseError,
}


def _reraise_kernel_error(exc: "_kernel.KernelError") -> "Error":
    """Convert a ``databricks_sql_kernel.KernelError`` to a PEP 249
    exception.

    Kernel errors carry their structured attrs (``code``,
    ``message``, ``sql_state``, ``error_code``, ``query_id`` …) as
    plain attributes — we copy them onto the re-raised exception so
    callers can branch on them without reaching back through
    ``__cause__``.
    """
    code = getattr(exc, "code", "Unknown")
    cls = _CODE_TO_EXCEPTION.get(code, DatabaseError)
    new = cls(getattr(exc, "message", str(exc)))
    # Forward the structured fields so connector users can read
    # err.sql_state / err.query_id / etc. without a type-switch.
    for attr in (
        "code",
        "sql_state",
        "error_code",
        "vendor_code",
        "http_status",
        "retryable",
        "query_id",
    ):
        setattr(new, attr, getattr(exc, attr, None))
    new.__cause__ = exc
    return new


# ─── Client ─────────────────────────────────────────────────────────────────


class KernelDatabricksClient(DatabricksClient):
    """``DatabricksClient`` that delegates to the Rust kernel.

    Owns one ``databricks_sql_kernel.Session`` per ``open_session``
    call. Async-execute handles (from ``submit()``) live in a dict
    keyed on ``CommandId`` so the connector's polling APIs
    (``get_query_state`` / ``get_execution_result`` /
    ``cancel_command`` / ``close_command``) can find them again.
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        auth_provider,
        ssl_options,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        http_headers=None,
        http_client=None,
        **kwargs,
    ):
        # The connector hands us several fields the kernel doesn't
        # consume directly (ssl_options, http_headers, http_client,
        # port). Kernel manages its own HTTP stack so we
        # accept-and-ignore.
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._auth_provider = auth_provider
        self._catalog = catalog
        self._schema = schema
        self._auth_kwargs = kernel_auth_kwargs(auth_provider)
        # Open ``databricks_sql_kernel.Session`` lazily in
        # ``open_session`` so the Session lifecycle gates the
        # underlying connection setup — same shape as Thrift's
        # ``TOpenSession``.
        self._kernel_session: Optional[Any] = None
        self._session_id: Optional[SessionId] = None
        # Async-exec handles keyed by CommandId.guid. Populated by
        # ``execute_command(async_op=True)``; drained by ``close_command``.
        # Guarded by ``_async_handles_lock`` so concurrent cursors on the
        # same connection don't race on submit / close / close-session.
        self._async_handles: Dict[str, Any] = {}
        self._async_handles_lock = threading.RLock()

    # ── Session lifecycle ──────────────────────────────────────────

    def open_session(
        self,
        session_configuration: Optional[Dict[str, Any]],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> SessionId:
        if self._kernel_session is not None:
            raise InterfaceError("KernelDatabricksClient already has an open session.")
        # ``session_configuration`` flows through to the kernel's
        # ``session_conf`` map verbatim; the SEA endpoint enforces
        # its own allow-list and rejects unknown keys.
        session_conf: Optional[Dict[str, str]] = None
        if session_configuration:
            session_conf = {k: str(v) for k, v in session_configuration.items()}
        try:
            self._kernel_session = _kernel.Session(
                host=self._server_hostname,
                http_path=self._http_path,
                catalog=catalog or self._catalog,
                schema=schema or self._schema,
                session_conf=session_conf,
                **self._auth_kwargs,
            )
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        finally:
            # Drop the raw access token from the instance once the
            # kernel session is constructed (or failed). The kernel
            # owns the credential from this point on; keeping a
            # cleartext copy on a long-lived connector object risks
            # accidental capture by pickling / debuggers / telemetry.
            self._auth_kwargs.pop("access_token", None)

        # Use the kernel's real server-issued session id, not a
        # synthetic UUID. Matches what the native SEA backend does.
        # Bind to a local first so mypy sees a non-Optional return.
        session_id = SessionId.from_sea_session_id(self._kernel_session.session_id)
        self._session_id = session_id
        logger.info("Opened kernel-backed session %s", session_id)
        return session_id

    def close_session(self, session_id: SessionId) -> None:
        if self._kernel_session is None:
            return
        # Close any tracked async handles first so they fire their
        # server-side CloseStatement before the session goes away.
        with self._async_handles_lock:
            handles_to_close = list(self._async_handles.values())
            self._async_handles.clear()
        for handle in handles_to_close:
            try:
                handle.close()
            except _kernel.KernelError as exc:
                logger.warning(
                    "Error closing async handle during session close: %s", exc
                )
        try:
            self._kernel_session.close()
        except _kernel.KernelError as exc:
            # Surface as a non-fatal warning — the kernel's Drop
            # impl will retry the close fire-and-forget. PEP 249
            # discourages raising from connection.close().
            logger.warning("Error closing kernel session: %s", exc)
        self._kernel_session = None
        self._session_id = None

    # ── Query execution ────────────────────────────────────────────

    def execute_command(
        self,
        operation: str,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        lz4_compression: bool,
        cursor: "Cursor",
        use_cloud_fetch: bool,
        parameters: List[ttypes.TSparkParameter],
        async_op: bool,
        enforce_embedded_schema_correctness: bool,
        row_limit: Optional[int] = None,
        query_tags: Optional[Dict[str, Optional[str]]] = None,
    ) -> Union["ResultSet", None]:
        if self._kernel_session is None:
            raise InterfaceError("Cannot execute_command without an open session.")
        if parameters:
            raise NotSupportedError(
                "Parameter binding is not yet supported on the kernel backend "
                "(PyO3 Statement.bind_param lands in a follow-up PR)."
            )
        if query_tags:
            raise NotSupportedError(
                "Statement-level query_tags are not yet supported on the kernel backend."
            )

        stmt = self._kernel_session.statement()
        try:
            stmt.set_sql(operation)
            if async_op:
                async_exec = stmt.submit()
                command_id = CommandId.from_sea_statement_id(async_exec.statement_id)
                cursor.active_command_id = command_id
                with self._async_handles_lock:
                    self._async_handles[command_id.guid] = async_exec
                return None
            executed = stmt.execute()
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        finally:
            # ``Statement`` is a lifecycle owner separate from the
            # executed handle it produces. Drop it here so the
            # parent doesn't keep the handle alive longer than the
            # caller expects.
            try:
                stmt.close()
            except _kernel.KernelError:
                pass

        command_id = CommandId.from_sea_statement_id(executed.statement_id)
        cursor.active_command_id = command_id
        return self._make_result_set(executed, cursor, command_id)

    def cancel_command(self, command_id: CommandId) -> None:
        with self._async_handles_lock:
            handle = self._async_handles.get(command_id.guid)
        if handle is None:
            # Sync-execute paths fully materialise the result before
            # ``execute_command`` returns, so by the time
            # cancel_command can fire there's nothing in flight.
            # Match the Thrift backend's tolerant behaviour.
            logger.debug("cancel_command: no in-flight async handle for %s", command_id)
            return
        try:
            handle.cancel()
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)

    def close_command(self, command_id: CommandId) -> None:
        with self._async_handles_lock:
            handle = self._async_handles.pop(command_id.guid, None)
        if handle is None:
            logger.debug("close_command: no tracked handle for %s", command_id)
            return
        try:
            handle.close()
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)

    def get_query_state(self, command_id: CommandId) -> CommandState:
        with self._async_handles_lock:
            handle = self._async_handles.get(command_id.guid)
        if handle is None:
            # No tracked async handle means execute_command ran
            # sync and the result was materialised before returning;
            # the command is terminal by construction.
            return CommandState.SUCCEEDED
        try:
            state, failure = handle.status()
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        if state == "Failed" and failure is not None:
            # Surface server-reported failure as a database error so
            # the cursor's polling loop terminates with the right
            # exception class — matches the Thrift backend's
            # behaviour on TOperationState::ERROR_STATE.
            raise _reraise_kernel_error(failure)
        return _STATE_TO_COMMAND_STATE.get(state, CommandState.FAILED)

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ) -> "ResultSet":
        with self._async_handles_lock:
            handle = self._async_handles.get(command_id.guid)
        if handle is None:
            raise ProgrammingError(
                "get_execution_result called for an unknown command_id; "
                "the kernel backend only tracks async-submitted statements."
            )
        try:
            stream = handle.await_result()
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        return self._make_result_set(stream, cursor, command_id)

    # ── Metadata ───────────────────────────────────────────────────

    def _make_result_set(
        self,
        kernel_handle: Any,
        cursor: "Cursor",
        command_id: CommandId,
    ) -> "ResultSet":
        """Build a ``KernelResultSet`` from any kernel handle. Used
        by sync execute, ``get_execution_result``, and all metadata
        paths to keep construction in one place."""
        return KernelResultSet(
            connection=cursor.connection,
            backend=self,
            kernel_handle=kernel_handle,
            command_id=command_id,
            arraysize=cursor.arraysize,
            buffer_size_bytes=cursor.buffer_size_bytes,
        )

    def _synthetic_command_id(self) -> CommandId:
        """Metadata calls don't produce a server statement id; mint
        a synthetic UUID so the ``ResultSet`` still has a stable
        identifier the cursor can attribute logs to.

        Plain ``uuid.uuid4().hex`` (no prefix) — anything that
        consumes ``cursor.query_id`` downstream (telemetry, log
        ingestion) sees a UUID-shaped string rather than a
        connector-internal magic prefix it cannot parse."""
        return CommandId.from_sea_statement_id(uuid.uuid4().hex)

    def get_catalogs(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
    ) -> "ResultSet":
        if self._kernel_session is None:
            raise InterfaceError("get_catalogs requires an open session.")
        try:
            stream = self._kernel_session.metadata().list_catalogs()
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        return self._make_result_set(stream, cursor, self._synthetic_command_id())

    def get_schemas(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> "ResultSet":
        if self._kernel_session is None:
            raise InterfaceError("get_schemas requires an open session.")
        try:
            stream = self._kernel_session.metadata().list_schemas(
                catalog=catalog_name,
                schema_pattern=schema_name,
            )
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        return self._make_result_set(stream, cursor, self._synthetic_command_id())

    def get_tables(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: Optional[List[str]] = None,
    ) -> "ResultSet":
        if self._kernel_session is None:
            raise InterfaceError("get_tables requires an open session.")
        try:
            stream = self._kernel_session.metadata().list_tables(
                catalog=catalog_name,
                schema_pattern=schema_name,
                table_pattern=table_name,
                table_types=table_types,
            )
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        if not table_types:
            return self._make_result_set(stream, cursor, self._synthetic_command_id())
        # The kernel today returns the unfiltered ``SHOW TABLES`` shape
        # regardless of ``table_types``. Drain to a single Arrow table
        # and apply the same client-side filter the native SEA backend
        # uses (column index 5 is TABLE_TYPE, case-sensitive). Cheap
        # because metadata result sets are small.
        from databricks.sql.backend.sea.utils.filters import ResultSetFilter

        full_table = _drain_kernel_handle(stream)
        filtered_table = ResultSetFilter._filter_arrow_table(
            full_table,
            column_name=full_table.schema.field(5).name,
            allowed_values=table_types,
            case_sensitive=True,
        )
        return self._make_result_set(
            _StaticArrowHandle(filtered_table),
            cursor,
            self._synthetic_command_id(),
        )

    def get_columns(
        self,
        session_id: SessionId,
        max_rows: int,
        max_bytes: int,
        cursor: "Cursor",
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> "ResultSet":
        if self._kernel_session is None:
            raise InterfaceError("get_columns requires an open session.")
        if not catalog_name:
            # Kernel's list_columns requires a catalog (SEA `SHOW
            # COLUMNS` cannot span catalogs). Surface the constraint
            # explicitly rather than letting the kernel error.
            raise ProgrammingError(
                "get_columns requires catalog_name on the kernel backend."
            )
        try:
            stream = self._kernel_session.metadata().list_columns(
                catalog=catalog_name,
                schema_pattern=schema_name,
                table_pattern=table_name,
                column_pattern=column_name,
            )
        except _kernel.KernelError as exc:
            raise _reraise_kernel_error(exc)
        return self._make_result_set(stream, cursor, self._synthetic_command_id())

    # ── Misc ───────────────────────────────────────────────────────

    @property
    def max_download_threads(self) -> int:
        # CloudFetch parallelism lives kernel-side. This property is
        # consulted by Thrift code paths that don't run for
        # use_kernel=True; return a non-zero default so anything that
        # peeks at it does not divide by zero.
        return 10


_STATE_TO_COMMAND_STATE: Dict[str, CommandState] = {
    "Pending": CommandState.PENDING,
    "Running": CommandState.RUNNING,
    "Succeeded": CommandState.SUCCEEDED,
    "Failed": CommandState.FAILED,
    "Cancelled": CommandState.CANCELLED,
    "Closed": CommandState.CLOSED,
}


def _drain_kernel_handle(handle: Any) -> Any:
    """Drain a kernel ResultStream / ExecutedStatement into a single
    ``pyarrow.Table``. Used by ``get_tables`` to apply a client-side
    ``table_types`` filter on a metadata result; cheap because
    metadata streams are small."""
    import pyarrow

    schema = handle.arrow_schema()
    batches = []
    while True:
        batch = handle.fetch_next_batch()
        if batch is None:
            break
        if batch.num_rows > 0:
            batches.append(batch)
    try:
        handle.close()
    except _kernel.KernelError:
        pass
    return pyarrow.Table.from_batches(batches, schema=schema)


class _StaticArrowHandle:
    """Duck-typed kernel handle that replays a pre-built
    ``pyarrow.Table`` through ``arrow_schema()`` /
    ``fetch_next_batch()`` / ``close()``. Used to wrap a
    post-processed table (e.g., the ``table_types``-filtered output
    of ``get_tables``) so it flows back through the normal
    ``KernelResultSet`` path."""

    def __init__(self, table: Any) -> None:
        self._schema = table.schema
        self._batches = list(table.to_batches())
        self._idx = 0

    def arrow_schema(self) -> Any:
        return self._schema

    def fetch_next_batch(self) -> Optional[Any]:
        if self._idx >= len(self._batches):
            return None
        batch = self._batches[self._idx]
        self._idx += 1
        return batch

    def close(self) -> None:
        self._batches = []
