"""DatabricksClient backed by the Rust ADBC kernel via PyO3 (POC).

Implements the connector's `DatabricksClient` interface by delegating to the
`databricks_adbc_pyo3` extension module, which loads the Rust kernel
(`databricks-adbc`) in-process. PAT-only for now; metadata and async
operations raise NotImplementedError.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import (
    BackendType,
    CommandId,
    CommandState,
    SessionId,
)
from databricks.sql.backend.adbc.result_set import AdbcResultSet
from databricks.sql.exc import DatabaseError, OperationalError, ProgrammingError
from databricks.sql.thrift_api.TCLIService import ttypes

if TYPE_CHECKING:
    from databricks.sql.client import Cursor
    from databricks.sql.result_set import ResultSet

logger = logging.getLogger(__name__)

try:
    import databricks_adbc_pyo3 as _rust_kernel
except ImportError as exc:  # pragma: no cover - import-time error surfaces clearly
    raise ImportError(
        "use_sea=True requires the databricks_adbc_pyo3 extension. Install it from "
        "the databricks-adbc/rust-pyo3 directory with `maturin develop --release` "
        "in your venv."
    ) from exc


class AdbcDatabricksClient(DatabricksClient):
    """DatabricksClient that routes execution through the Rust ADBC kernel.

    Construction does not open a Rust connection — that happens in
    `open_session` so the same Session lifecycle that today gates Thrift's
    `TOpenSession` gates the Rust kernel's connection setup too.
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs,
    ):
        if not access_token:
            raise ProgrammingError(
                "AdbcDatabricksClient (use_sea=True) currently supports only PAT auth. "
                "Pass access_token=<dapi...>."
            )
        # Auth provider is built upstream but the Rust kernel re-does PAT auth itself,
        # so we just need the raw token here.
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token
        self._initial_catalog = catalog
        self._initial_schema = schema

        # Per-session state. We support a single open session at a time; opening
        # a second one will raise. Matches the current Session lifecycle.
        self._connection: Optional[_rust_kernel.Connection] = None
        self._session_id: Optional[SessionId] = None

    # ----- session lifecycle -----

    def open_session(
        self,
        session_configuration: Optional[Dict[str, Any]],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> SessionId:
        if self._connection is not None:
            raise OperationalError("AdbcDatabricksClient already has an open session.")
        if session_configuration:
            logger.warning(
                "AdbcDatabricksClient ignores session_configuration in POC: %s",
                list(session_configuration.keys()),
            )
        try:
            self._connection = _rust_kernel.Connection(
                self._server_hostname,
                self._http_path,
                self._access_token,
                catalog=catalog or self._initial_catalog,
                schema=schema or self._initial_schema,
            )
        except RuntimeError as exc:
            raise OperationalError(f"Failed to open Rust ADBC session: {exc}") from exc

        # Mint a synthetic SEA-style session id; the kernel manages real session
        # lifecycle internally and does not surface its session GUID today.
        self._session_id = SessionId.from_sea_session_id(str(uuid.uuid4()))
        logger.info("Opened ADBC-Rust session %s", self._session_id)
        return self._session_id

    def close_session(self, session_id: SessionId) -> None:
        if self._connection is None:
            return
        # PyO3 Connection has no explicit close in the POC — drop the reference
        # and let Drop release the Rust-side resources.
        self._connection = None
        self._session_id = None

    # ----- query execution -----

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
        if self._connection is None:
            raise OperationalError("Cannot execute_command on closed session.")
        if async_op:
            raise NotImplementedError(
                "async_op is not supported by the Rust ADBC backend (POC)."
            )
        if parameters:
            raise NotImplementedError(
                "Parameter binding is not supported by the Rust ADBC backend (POC)."
            )

        try:
            rs = self._connection.execute(operation)
        except RuntimeError as exc:
            raise DatabaseError(f"Rust ADBC execution failed: {exc}") from exc

        # The kernel does not surface its statement_id today; mint a synthetic one.
        command_id = CommandId.from_sea_statement_id(str(uuid.uuid4()))
        cursor.active_command_id = command_id

        return AdbcResultSet(
            connection=cursor.connection,
            backend=self,
            rust_result_set=rs,
            command_id=command_id,
            arraysize=cursor.arraysize,
            buffer_size_bytes=cursor.buffer_size_bytes,
        )

    def cancel_command(self, command_id: CommandId) -> None:
        # POC: execute_command is fully synchronous and the result is materialized
        # before it returns, so there is nothing to cancel after the fact.
        logger.debug("cancel_command is a no-op in the Rust ADBC POC backend")

    def close_command(self, command_id: CommandId) -> None:
        # Result set is already drained on the Rust side.
        logger.debug("close_command is a no-op in the Rust ADBC POC backend")

    def get_query_state(self, command_id: CommandId) -> CommandState:
        # All commands run synchronously and reach SUCCEEDED before returning.
        return CommandState.SUCCEEDED

    def get_execution_result(
        self,
        command_id: CommandId,
        cursor: "Cursor",
    ) -> "ResultSet":
        raise NotImplementedError(
            "get_execution_result requires async execution (not supported in POC)."
        )

    # ----- metadata (not yet wired) -----

    def get_catalogs(self, *args, **kwargs):
        raise NotImplementedError("get_catalogs is not supported by the Rust ADBC backend (POC).")

    def get_schemas(self, *args, **kwargs):
        raise NotImplementedError("get_schemas is not supported by the Rust ADBC backend (POC).")

    def get_tables(self, *args, **kwargs):
        raise NotImplementedError("get_tables is not supported by the Rust ADBC backend (POC).")

    def get_columns(self, *args, **kwargs):
        raise NotImplementedError("get_columns is not supported by the Rust ADBC backend (POC).")

    @property
    def max_download_threads(self) -> int:
        # The kernel manages its own CloudFetch parallelism; this property is
        # only consulted by Thrift code paths that don't run for use_sea=True.
        return 10
