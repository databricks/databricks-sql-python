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
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Union

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.kernel._errors import (
    _kernel,
    reraise_kernel_error as _reraise_kernel_error,
    wrap_kernel_exception as _wrap_kernel_exception,
)
from databricks.sql.backend.kernel.auth_bridge import kernel_auth_kwargs
from databricks.sql.backend.kernel.result_set import KernelResultSet
from databricks.sql.backend.kernel.type_mapping import bind_tspark_params
from databricks.sql.backend.types import (
    BackendType,
    CommandId,
    CommandState,
    SessionId,
)
from databricks.sql.exc import (
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

# Headers the kernel manages itself and that the connector must NOT
# forward via ``http_headers`` (lower-cased for case-insensitive match):
# ``authorization`` (the kernel applies the auth provider's token) and
# ``x-databricks-org-id`` (the kernel re-derives it from the ``?o=`` in
# http_path). Forwarding either is redundant and trips the kernel's
# per-request skip-and-warn.
_KERNEL_MANAGED_HEADERS = frozenset({"authorization", "x-databricks-org-id"})


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
        # ``ssl_options`` is translated to the kernel's ``tls_*``
        # Session kwargs in ``open_session`` (custom CA, verify
        # toggles, mTLS client cert/key). ``http_headers`` is forwarded
        # to the kernel as custom request headers (it carries the
        # connector's composed ``User-Agent`` + any caller headers + the
        # SPOG ``x-databricks-org-id``). ``http_client`` / ``port`` are
        # still accept-and-ignore — the kernel manages its own HTTP
        # stack.
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._auth_provider = auth_provider
        self._ssl_options = ssl_options
        # Caller / connector HTTP headers (list of (name, value) pairs).
        # Forwarded to the kernel Session in ``open_session``.
        self._http_headers = http_headers or []
        # Raw auth-relevant connect() kwargs (auth_type,
        # oauth_client_id/secret, redirect port, credentials_provider).
        # The kernel auth bridge needs these to build OAuth kwargs — the
        # OAuth secret is consumed during ``auth_provider`` construction
        # and isn't recoverable from the built provider.
        self._auth_options = kwargs.get("auth_options") or {}
        # Connector retry-tuning kwargs (the ``_retry_*`` family),
        # forwarded so the kernel's own retry loop honours them. Mapped
        # to the kernel ``Session``'s ``retry_*`` kwargs in
        # ``open_session`` via ``_kernel_retry_kwargs``.
        self._retry_options = kwargs.get("retry_options") or {}
        self._catalog = catalog
        self._schema = schema
        # ``_use_arrow_native_complex_types`` is the connector-side
        # toggle for whether complex columns (ARRAY / MAP / STRUCT)
        # are surfaced as native Arrow shapes or as compact JSON
        # strings. The Thrift backend forwards it server-side
        # (``complexTypesAsArrow``); the kernel doesn't have a wire
        # equivalent, so we flip the kernel's client-side
        # ``complex_types_as_json`` post-processor to match. Default
        # ``True`` mirrors the connector's existing default.
        self._use_arrow_native_complex_types = kwargs.get(
            "_use_arrow_native_complex_types", True
        )
        # NB: don't call ``kernel_auth_kwargs`` here. That call
        # materialises the bearer token in-process; keeping a
        # cleartext copy on a long-lived connector object that may
        # never have ``open_session`` invoked (test paths, error
        # paths, lazy retries) widens the window where a debugger
        # dump or accidental pickle could capture the credential.
        # Resolved inside ``open_session`` instead, then immediately
        # cleared once the kernel ``Session`` owns it.
        #
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
        # Parent ``Statement`` objects kept alive alongside async handles.
        # On the kernel, ``Statement.close()`` flips the validity flag on
        # the produced executed handle (see kernel
        # ``statement::mutable::close``), so we cannot close the
        # Statement immediately after ``submit()`` as we do for sync
        # ``execute()``. Instead retain it here and close it in
        # ``close_command`` / ``close_session`` after the async handle
        # has finished its work.
        self._async_statements: Dict[str, Any] = {}
        # CommandId.guids of async commands that have already been
        # closed (via ``close_command`` or ``close_session``). Lets
        # ``get_query_state`` report ``CLOSED`` for them rather than
        # the SUCCEEDED fall-through used for the never-tracked sync
        # path. Same lock as ``_async_handles``.
        self._closed_commands: Set[str] = set()
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
        # The kwarg builds run INSIDE the try so the ``finally`` scrub
        # below always fires — including when ``kernel_auth_kwargs``
        # itself raises mid-build (e.g. an OAuth token-exchange failure
        # while the M2M secret is in hand). Pre-declared empty so the
        # ``finally`` can reference them unconditionally even on an early
        # raise. Building here (not in ``__init__``) keeps the bearer
        # token's in-process lifetime as short as possible.
        auth_kwargs: Dict[str, Any] = {}
        tls_kwargs: Dict[str, Any] = {}
        try:
            auth_kwargs = kernel_auth_kwargs(self._auth_provider, self._auth_options)
            # Translate the connector's SSLOptions into the kernel's
            # ``tls_*`` Session kwargs. Empty when TLS is at defaults.
            tls_kwargs = _kernel_tls_kwargs(self._ssl_options)
            # Translate the connector's ``_retry_*`` kwargs into the
            # kernel's ``retry_*`` kwargs. Empty when at defaults.
            retry_kwargs = _kernel_retry_kwargs(self._retry_options)
            # Forward caller / connector HTTP headers. The kernel applies
            # them on every request; a caller ``User-Agent`` is appended
            # to the kernel's base UA. Only pass the kwarg when there's
            # something to send.
            #
            # We drop ``Authorization`` and ``x-databricks-org-id`` here,
            # before they reach the kernel, for two reasons: (1) the
            # kernel manages both itself (auth from the provider; org-id
            # re-derived from the ``?o=`` in http_path), so forwarding
            # them is redundant; (2) the kernel skips-and-warns those two
            # names on every request, so forwarding the SPOG org-id the
            # connector always injects would spam a warning per request.
            # This double-walls the kernel's own reserved-name skip.
            http_headers_kwargs: Dict[str, Any] = {}
            if self._http_headers:
                forwarded = [
                    (str(k), str(v))
                    for k, v in self._http_headers
                    if str(k).lower() not in _KERNEL_MANAGED_HEADERS
                ]
                if forwarded:
                    http_headers_kwargs["http_headers"] = forwarded
            self._kernel_session = _kernel.Session(
                host=self._server_hostname,
                http_path=self._http_path,
                catalog=catalog or self._catalog,
                schema=schema or self._schema,
                session_conf=session_conf,
                complex_types_as_json=not self._use_arrow_native_complex_types,
                # Pyarrow's Python bindings cannot decode Arrow's
                # ``month_interval`` type at all (id 21 — raises
                # ``KeyError`` from ``.as_py``, ``to_pylist``,
                # ``cast(string)``, and ``to_pandas``). Ask the kernel
                # to stringify INTERVAL / DURATION columns server-side
                # so result sets containing interval columns are
                # decodable on the Python side. Matches the Thrift
                # backend's surface (interval columns arrive as
                # strings).
                intervals_as_string=True,
                **auth_kwargs,
                **tls_kwargs,
                **retry_kwargs,
                **http_headers_kwargs,
            )
        except Exception as exc:
            raise _wrap_kernel_exception("open_session", exc) from exc
        finally:
            # Best-effort scrub of the local dicts before they go out
            # of scope. The kernel ``Session`` (if construction
            # succeeded) now owns its own copies. ``access_token``
            # (PAT), ``client_secret`` (M2M), and the mTLS client key
            # bytes are all credential material.
            auth_kwargs.pop("access_token", None)
            auth_kwargs.pop("client_secret", None)
            tls_kwargs.pop("tls_client_key", None)
            # Also scrub the long-lived copy. ``self._auth_options``
            # outlives this method (it's set in ``__init__`` and the
            # connector object can live for the whole connection), so a
            # retained ``oauth_client_secret`` would be exposed to
            # ``vars(conn)`` / pickle / a debugger dump for far longer
            # than the credential needs to exist. The kernel now owns
            # the secret, so drop ours.
            self._auth_options.pop("oauth_client_secret", None)

        # Use the kernel's real server-issued session id, not a
        # synthetic UUID. Matches what the native SEA backend does.
        # ``session_id`` is a PyO3 attribute access — also wrapped so
        # any conversion error surfaces as a mapped PEP 249 exception
        # instead of bubbling raw from the boundary.
        try:
            session_id = SessionId.from_sea_session_id(self._kernel_session.session_id)
        except Exception as exc:
            raise _wrap_kernel_exception("open_session", exc) from exc
        self._session_id = session_id
        logger.info("Opened kernel-backed session %s", session_id)
        return session_id

    def close_session(self, session_id: SessionId) -> None:
        if self._kernel_session is None:
            return
        # Close any tracked async handles first so they fire their
        # server-side CloseStatement before the session goes away.
        with self._async_handles_lock:
            tracked = list(self._async_handles.items())
            tracked_stmts = list(self._async_statements.items())
            self._async_handles.clear()
            self._async_statements.clear()
            for guid, _ in tracked:
                self._closed_commands.add(guid)
        for _, handle in tracked:
            # Per-handle close errors are non-fatal — PEP 249
            # discourages raising from session close — so log and
            # move on. Any non-KernelError that crosses the PyO3
            # boundary also gets caught here for the same reason.
            try:
                handle.close()
            except Exception as exc:
                logger.warning(
                    "Error closing async handle during session close: %s", exc
                )
        # Now drop the parent Statements that were keeping those handles
        # alive. Same non-fatal close semantics — close errors are not
        # actionable at session-close time.
        for _, stmt in tracked_stmts:
            try:
                stmt.close()
            except Exception as exc:
                logger.warning(
                    "Error closing async statement during session close: %s", exc
                )
        try:
            self._kernel_session.close()
        except Exception as exc:
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

        try:
            stmt = self._kernel_session.statement()
        except Exception as exc:
            raise _wrap_kernel_exception("execute_command", exc) from exc
        # ``async_op`` keeps ``stmt`` alive (tracked in
        # ``_async_statements`` and closed by ``close_command``); the sync
        # path drops it in finally. ``close_stmt`` is the post-success
        # decision flag — it stays True on sync, flips to False on async.
        close_stmt = True
        try:
            try:
                stmt.set_sql(operation)
                if query_tags:
                    # Per-statement query tags. The kernel serialises the
                    # dict (None value -> bare key) into the SEA
                    # `query_tags` statement conf. ``query_tags`` is
                    # already ``Dict[str, Optional[str]]`` from the
                    # connector, which the kernel accepts directly.
                    stmt.set_query_tags(query_tags)
                if parameters:
                    bind_tspark_params(stmt, parameters)
                if async_op:
                    async_exec = stmt.submit()
                    command_id = CommandId.from_sea_statement_id(
                        async_exec.statement_id
                    )
                    cursor.active_command_id = command_id
                    with self._async_handles_lock:
                        self._async_handles[command_id.guid] = async_exec
                        # Closing the kernel ``Statement`` invalidates the
                        # async handle (see kernel validity flag). Retain
                        # the Statement here and close it on
                        # ``close_command`` / ``close_session``.
                        self._async_statements[command_id.guid] = stmt
                    close_stmt = False
                    return None
                executed = stmt.execute()
            except Exception as exc:
                raise _wrap_kernel_exception("execute_command", exc) from exc
        finally:
            if close_stmt:
                # Sync path: ``Statement`` is a lifecycle owner separate
                # from the executed handle. Drop it here so the parent
                # doesn't outlive its caller. Swallow close errors —
                # they're not actionable.
                try:
                    stmt.close()
                except Exception:
                    pass

        command_id = CommandId.from_sea_statement_id(executed.statement_id)
        cursor.active_command_id = command_id
        # ``KernelResultSet.__init__`` calls ``arrow_schema()`` which
        # can itself raise ``KernelError`` (or, in principle, a PyO3
        # native exception) — wrap the construction so callers see a
        # mapped PEP 249 exception.
        try:
            return self._make_result_set(executed, cursor, command_id)
        except Exception as exc:
            raise _wrap_kernel_exception("execute_command", exc) from exc

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
        except Exception as exc:
            raise _wrap_kernel_exception("cancel_command", exc) from exc

    def close_command(self, command_id: CommandId) -> None:
        with self._async_handles_lock:
            handle = self._async_handles.pop(command_id.guid, None)
            stmt = self._async_statements.pop(command_id.guid, None)
            if handle is not None:
                # Record the close so ``get_query_state`` can report
                # ``CLOSED`` (not ``SUCCEEDED``) for this command.
                self._closed_commands.add(command_id.guid)
        if handle is None:
            logger.debug("close_command: no tracked handle for %s", command_id)
            # Still drop the parent Statement if somehow tracked without
            # the handle — keeps the invariant clean even on bookkeeping
            # races.
            if stmt is not None:
                try:
                    stmt.close()
                except Exception:
                    pass
            return
        try:
            handle.close()
        except Exception as exc:
            raise _wrap_kernel_exception("close_command", exc) from exc
        finally:
            # Now safe to close the parent Statement — the executed
            # handle has finished its lifecycle.
            if stmt is not None:
                try:
                    stmt.close()
                except Exception:
                    pass

    def get_query_state(self, command_id: CommandId) -> CommandState:
        with self._async_handles_lock:
            handle = self._async_handles.get(command_id.guid)
            already_closed = command_id.guid in self._closed_commands
        if handle is None:
            if already_closed:
                # We tracked this async handle and have since closed
                # it; the command is no longer queryable on the
                # server but the connector still has the id.
                return CommandState.CLOSED
            # No tracked async handle and never closed: execute_command
            # ran sync and the result was materialised before
            # returning. Terminal by construction.
            return CommandState.SUCCEEDED
        try:
            state, failure = handle.status()
        except Exception as exc:
            raise _wrap_kernel_exception("get_query_state", exc) from exc
        if state == "Failed" and failure is not None:
            # Surface server-reported failure as a database error so
            # the cursor's polling loop terminates with the right
            # exception class — matches the Thrift backend's
            # behaviour on TOperationState::ERROR_STATE. Routed
            # through ``_wrap_kernel_exception`` rather than
            # ``_reraise_kernel_error`` directly so a non-
            # ``KernelError``-shaped ``failure`` (kernel API drift —
            # struct, dict, etc.) still produces a mapped PEP 249
            # exception instead of a confusing
            # ``TypeError: exception causes must derive from
            # BaseException`` from the ``from`` clause.
            if isinstance(failure, BaseException):
                raise _wrap_kernel_exception("get_query_state", failure) from failure
            raise _wrap_kernel_exception("get_query_state", Exception(repr(failure)))
        return _STATE_TO_COMMAND_STATE.get(state, CommandState.FAILED)

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ) -> "ResultSet":
        with self._async_handles_lock:
            async_exec = self._async_handles.get(command_id.guid)
        if async_exec is None:
            raise ProgrammingError(
                "get_execution_result called for an unknown command_id; "
                "the kernel backend only tracks async-submitted statements."
            )
        try:
            stream = async_exec.await_result()
        except Exception as exc:
            raise _wrap_kernel_exception("get_execution_result", exc) from exc
        # The async-exec handle's role ends once it has produced the
        # ``ResultStream`` — keeping it around (and tracked in
        # ``_async_handles``) would leak the server-side
        # ``ExecutedAsyncStatement`` until ``close_session`` swept it
        # up, since ``KernelResultSet.close`` only closes the stream
        # it wraps. Drop tracking and fire-and-forget the close.
        with self._async_handles_lock:
            self._async_handles.pop(command_id.guid, None)
            stmt = self._async_statements.pop(command_id.guid, None)
            self._closed_commands.add(command_id.guid)
        try:
            async_exec.close()
        except Exception as exc:
            logger.warning(
                "Error closing async_exec after await_result for %s: %s",
                command_id,
                exc,
            )
        # The parent Statement is no longer needed once the async handle
        # has produced its ResultStream. Close to release server-side
        # tracking; matches the sync path's eager Statement close.
        if stmt is not None:
            try:
                stmt.close()
            except Exception as exc:
                logger.warning(
                    "Error closing async statement after await_result for %s: %s",
                    command_id,
                    exc,
                )
        # ``KernelResultSet.__init__`` calls ``arrow_schema()`` which
        # can raise — map that to PEP 249 too.
        try:
            return self._make_result_set(stream, cursor, command_id)
        except Exception as exc:
            raise _wrap_kernel_exception("get_execution_result", exc) from exc

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
            return self._make_result_set(stream, cursor, self._synthetic_command_id())
        except Exception as exc:
            raise _wrap_kernel_exception("get_catalogs", exc) from exc

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
            return self._make_result_set(stream, cursor, self._synthetic_command_id())
        except Exception as exc:
            raise _wrap_kernel_exception("get_schemas", exc) from exc

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
            if not table_types:
                return self._make_result_set(
                    stream, cursor, self._synthetic_command_id()
                )
            # The kernel today returns the unfiltered ``SHOW TABLES``
            # shape regardless of ``table_types``. Drain to a single
            # Arrow table and apply the same client-side filter the
            # native SEA backend uses. The filter is **case-sensitive**
            # — matches the SEA backend's documented behaviour, and
            # mirrors how the warehouse reports the values
            # (``TABLE`` / ``VIEW`` / ``SYSTEM_TABLE`` — uppercase).
            # Look the column up by name rather than positional index
            # so a future kernel reshape of ``SHOW TABLES`` doesn't
            # silently filter the wrong column.
            from databricks.sql.backend.sea.utils.filters import ResultSetFilter

            full_table = _drain_kernel_handle(stream)
            if "TABLE_TYPE" not in full_table.schema.names:
                raise OperationalError(
                    "kernel get_tables result is missing a TABLE_TYPE "
                    f"column; got {full_table.schema.names!r}"
                )
            filtered_table = ResultSetFilter._filter_arrow_table(
                full_table,
                column_name="TABLE_TYPE",
                allowed_values=table_types,
                case_sensitive=True,
            )
            return self._make_result_set(
                _StaticArrowHandle(filtered_table),
                cursor,
                self._synthetic_command_id(),
            )
        except Exception as exc:
            raise _wrap_kernel_exception("get_tables", exc) from exc

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
        try:
            # `catalog_name=None` is supported: the kernel issues
            # `SHOW COLUMNS IN ALL CATALOGS` server-side and the
            # response carries `catalogName` per row, so each result
            # row's `TABLE_CAT` is correctly attributed. Matches the
            # Thrift backend's `getColumns(null, …)` behaviour from
            # the user's perspective.
            stream = self._kernel_session.metadata().list_columns(
                catalog=catalog_name,
                schema_pattern=schema_name,
                table_pattern=table_name,
                column_pattern=column_name,
            )
            return self._make_result_set(stream, cursor, self._synthetic_command_id())
        except Exception as exc:
            raise _wrap_kernel_exception("get_columns", exc) from exc

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


def _kernel_tls_kwargs(ssl_options) -> Dict[str, Any]:
    """Translate the connector's ``SSLOptions`` into the kernel
    ``Session``'s ``tls_*`` kwargs.

    Only non-default settings are emitted, so a stock TLS config
    produces an empty dict (kernel keeps its secure default: validate
    the system trust store, verify chain + hostname).

    Mappings (note the inverted booleans — the connector expresses
    *verify*, the kernel expresses *skip*):

    - ``tls_verify=False``          → ``tls_skip_verify=True`` **and**
      ``tls_skip_hostname_verify=True``
    - ``tls_verify_hostname=False`` → ``tls_skip_hostname_verify=True``
    - ``tls_trusted_ca_file``       → ``tls_ca_cert`` (PEM bytes)
    - ``tls_client_cert_file`` (+ key file) → ``tls_client_cert`` /
      ``tls_client_key`` (PEM bytes) for mutual TLS.

    The connector's certificate files are read into bytes here because
    the kernel's pyo3 surface takes in-memory PEM, not paths.
    """
    if ssl_options is None:
        return {}

    kwargs: Dict[str, Any] = {}

    # Inverted booleans. Emit only the insecure (skip) direction so the
    # default secure path stays implicit. ``tls_verify=False`` disables
    # all chain validation, which subsumes hostname verification — so we
    # also emit ``tls_skip_hostname_verify`` to match ``SSLOptions``'s
    # own semantics (``create_ssl_context`` sets ``check_hostname=False``
    # whenever ``tls_verify`` is False). Without this the kernel could
    # still attempt a hostname check the connector considers disabled.
    if getattr(ssl_options, "tls_verify", True) is False:
        kwargs["tls_skip_verify"] = True
        kwargs["tls_skip_hostname_verify"] = True
    elif getattr(ssl_options, "tls_verify_hostname", True) is False:
        kwargs["tls_skip_hostname_verify"] = True

    ca_file = getattr(ssl_options, "tls_trusted_ca_file", None)
    if ca_file:
        kwargs["tls_ca_cert"] = _read_pem_bytes(ca_file, "tls_trusted_ca_file")

    cert_file = getattr(ssl_options, "tls_client_cert_file", None)
    key_file = getattr(ssl_options, "tls_client_cert_key_file", None)
    if cert_file:
        # The kernel pairs cert + key for mutual TLS; a cert without a
        # key (or vice versa) is rejected kernel-side. The connector's
        # SSLOptions allows a combined cert+key file (key_file None), so
        # fall back to the cert file for the key in that case.
        kwargs["tls_client_cert"] = _read_pem_bytes(cert_file, "tls_client_cert_file")
        kwargs["tls_client_key"] = _read_pem_bytes(
            key_file or cert_file, "tls_client_cert_key_file"
        )
        # The kernel has no surface for an encrypted client key today.
        # Reject loudly rather than hand the kernel a key it can't
        # decrypt (which would fail with an opaque TLS parse error).
        if getattr(ssl_options, "tls_client_cert_key_password", None):
            raise NotSupportedError(
                "use_kernel=True does not support a password-protected mTLS "
                "client key (tls_client_cert_key_password). Provide an "
                "unencrypted PEM key, or use the Thrift backend (default)."
            )

    return kwargs


def _kernel_retry_kwargs(retry_options: Dict[str, Any]) -> Dict[str, Any]:
    """Translate the connector's ``_retry_*`` tuning into the kernel
    ``Session``'s ``retry_*`` kwargs.

    Only knobs the caller actually set are emitted, so an untuned
    connection produces an empty dict (kernel keeps its default policy:
    1s/60s backoff, 6 total attempts, 900s budget).

    Mappings (connector → kernel):

    - ``retry_delay_min`` (float secs) → ``retry_min_wait_secs``
    - ``retry_delay_max`` (float secs) → ``retry_max_wait_secs``
    - ``retry_stop_after_attempts_count`` (int, **total** attempts) →
      ``retry_max_attempts`` (1:1 — the kernel converts to its
      retries-after-first internally)
    - ``retry_stop_after_attempts_duration`` (float secs) →
      ``retry_overall_timeout_secs``

    The connector expresses delays/durations as **floats in seconds**;
    the kernel takes **whole seconds** (``u64``). We round to the
    nearest second, with a floor of 1s for any positive sub-second
    value so a configured delay never collapses to "no wait".

    ``_retry_delay_default`` has no kernel counterpart and is ignored:
    the kernel's no-``Retry-After`` backoff is exponential from
    ``retry_min_wait``, which already plays that role.
    """
    kwargs: Dict[str, Any] = {}

    def _secs(value: Any) -> Optional[int]:
        if value is None:
            return None
        rounded = round(float(value))
        # Never round a positive delay down to 0 — that would turn a
        # configured backoff into a busy-retry. Floor at 1s.
        if rounded <= 0 and float(value) > 0:
            return 1
        return rounded

    min_wait = _secs(retry_options.get("retry_delay_min"))
    if min_wait is not None:
        kwargs["retry_min_wait_secs"] = min_wait

    max_wait = _secs(retry_options.get("retry_delay_max"))
    if max_wait is not None:
        kwargs["retry_max_wait_secs"] = max_wait

    count = retry_options.get("retry_stop_after_attempts_count")
    if count is not None:
        # Total-attempts count, forwarded 1:1; the kernel converts to
        # its retries-after-first representation.
        kwargs["retry_max_attempts"] = int(count)

    duration = _secs(retry_options.get("retry_stop_after_attempts_duration"))
    if duration is not None:
        kwargs["retry_overall_timeout_secs"] = duration

    return kwargs


def _read_pem_bytes(path: str, label: str) -> bytes:
    """Read a PEM file into bytes, mapping IO errors to a clear
    ``ProgrammingError`` that names the offending TLS option. An empty
    file is rejected here too — otherwise it reaches the kernel as
    empty PEM and surfaces as a cryptic ``no certificates found`` /
    parse error far from the misconfigured path."""
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as exc:
        raise ProgrammingError(
            f"Failed to read {label} '{path}' for the kernel TLS config: {exc}"
        ) from exc
    if not data.strip():
        raise ProgrammingError(
            f"{label} '{path}' is empty; expected PEM-encoded content for the "
            "kernel TLS config."
        )
    return data


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
    except Exception:
        # Non-fatal — the surrounding ``get_tables`` call has already
        # captured the result data, and the handle's server-side
        # state will be reaped by the kernel's Drop impl.
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
