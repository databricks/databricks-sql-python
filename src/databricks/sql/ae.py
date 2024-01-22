from enum import Enum
from typing import Optional, Union, TYPE_CHECKING
from databricks.sql.exc import RequestError
from databricks.sql.results import ResultSet, execute_response_contains_direct_results

from datetime import datetime

from dataclasses import dataclass

if TYPE_CHECKING:
    from databricks.sql.thrift_backend import ThriftBackend
    from databricks.sql.client import Connection
    from databricks.sql.results import ResultSet


from uuid import UUID

from databricks.sql.thrift_api.TCLIService import ttypes


class AsyncExecutionException(Exception):
    pass


class AsyncExecutionUnrecoverableResultException(AsyncExecutionException):
    """Raised when a result can never be retrieved for this query id."""

    pass


@dataclass
class FakeCursor:
    active_op_handle: Optional[ttypes.TOperationHandle]


@dataclass
class FakeExecuteStatementResponse:
    directResults: bool
    operationHandle: ttypes.TOperationHandle


class AsyncExecutionStatus(Enum):
    """An enum that represents the status of an async execution"""

    PENDING = 0
    RUNNING = 1
    FINISHED = 2
    CANCELED = 3
    FETCHED = 4

    # todo: when is this ever evaluated?
    ABORTED = 5
    UNKNOWN = 6


def _toperationstate_to_ae_status(
    input: ttypes.TOperationState,
) -> AsyncExecutionStatus:
    x = ttypes.TOperationState

    if input in [x.INITIALIZED_STATE, x.PENDING_STATE, x.RUNNING_STATE]:
        return AsyncExecutionStatus.RUNNING
    if input == x.CANCELED_STATE:
        return AsyncExecutionStatus.CANCELED
    if input == x.FINISHED_STATE:
        return AsyncExecutionStatus.FINISHED
    if input in [x.CLOSED_STATE, x.ERROR_STATE, x.UKNOWN_STATE, x.TIMEDOUT_STATE]:
        return AsyncExecutionStatus.ABORTED


class AsyncExecution:
    """
    A handle for a query execution on Databricks.
    """

    _connection: "Connection"
    _thrift_backend: "ThriftBackend"
    _result_set: Optional["ResultSet"]
    _execute_statement_response: Optional[
        Union[FakeExecuteStatementResponse, ttypes.TExecuteStatementResp]
    ]
    _last_sync_timestamp: Optional[datetime] = None
    _result_set: Optional["ResultSet"] = None
    _is_available: bool = True

    def __init__(
        self,
        thrift_backend: "ThriftBackend",
        connection: "Connection",
        query_id: UUID,
        query_secret: UUID,
        status: Optional[AsyncExecutionStatus] = AsyncExecutionStatus.UNKNOWN,
        execute_statement_response: Optional[
            Union[FakeExecuteStatementResponse, ttypes.TExecuteStatementResp]
        ] = None,
    ):
        self._connection = connection
        self._thrift_backend = thrift_backend
        self.query_id = query_id
        self.query_secret = query_secret
        self.status = status

        if execute_statement_response:
            self._execute_statement_response = execute_statement_response
            if execute_response_contains_direct_results(execute_statement_response):
                self._is_available = False
        else:
            self._execute_statement_response = FakeExecuteStatementResponse(
                directResults=False, operationHandle=self.t_operation_handle
            )

    status: AsyncExecutionStatus
    query_id: UUID

    def get_result(
        self,
    ) -> "ResultSet":
        """Attempt to get the result of this query and set self.status to FETCHED.

        IMPORTANT: Generally, you'll call this method only after checking that the query is finished.
        But you can call it at any time. If you call this method while the query is still running,
        your code will block indefinitely until the query completes! This will be changed in a
        subsequent release (PECO-1291)

        If you have already called get_result successfully, this method will return the same ResultSet
        as before without making an additional roundtrip to the server.

        Raises an AsyncExecutionUnrecoverableResultException if the query was canceled or aborted
            at the server, so a result will never be available.
        """

        # this isn't recoverable
        if self.status in [AsyncExecutionStatus.ABORTED, AsyncExecutionStatus.CANCELED]:
            raise AsyncExecutionUnrecoverableResultException(
                "Result for %s is not recoverable. Query status is %s"
                % (self.query_id, self.status),
            )

        return self._get_result_set()

    def _get_result_set(self) -> "ResultSet":
        if self._result_set is None:
            self._result_set = self._thrift_fetch_result()
            self.status = AsyncExecutionStatus.FETCHED

        return self._result_set

    def cancel(self) -> None:
        """Cancel the query"""
        self._thrift_cancel_operation()

    def _thrift_cancel_operation(self) -> None:
        """Execute TCancelOperation"""

        _output = self._thrift_backend.async_cancel_command(self.t_operation_handle)
        self.status = AsyncExecutionStatus.CANCELED

    def _thrift_get_operation_status(self) -> ttypes.TGetOperationStatusResp:
        """Execute TGetOperationStatusReq

        Raises an AsyncExecutionError if the query_id:query_secret pair is not found on the server.
        """
        try:
            return self._thrift_backend._poll_for_status(self.t_operation_handle)
        except RequestError as e:
            if "RESOURCE_DOES_NOT_EXIST" in e.message:
                raise AsyncExecutionException(
                    "Query not found: %s" % self.query_id
                ) from e

    def serialize(self) -> str:
        """Return a string representing the query_id and secret of this AsyncExecution.

        Use this to preserve a reference to the query_id and query_secret."""
        return f"{self.query_id}:{self.query_secret}"

    def sync_status(self) -> None:
        """Synchronise the status of this AsyncExecution with the server query execution state."""

        resp = self._thrift_get_operation_status()
        self.status = _toperationstate_to_ae_status(resp.operationState)
        self._last_sync_timestamp = datetime.now()

    def _thrift_fetch_result(self) -> "ResultSet":
        """Execute TFetchResultReq"""

        er = self._thrift_backend._handle_execute_response(
            self._execute_statement_response, FakeCursor(None)
        )

        return ResultSet(
            connection=self._connection,
            execute_response=er,
            thrift_backend=self._connection.thrift_backend,
        )

    @property
    def is_running(self) -> bool:
        return self.status in [
            AsyncExecutionStatus.RUNNING,
            AsyncExecutionStatus.PENDING,
        ]

    @property
    def is_canceled(self) -> bool:
        return self.status == AsyncExecutionStatus.CANCELED

    @property
    def is_finished(self) -> bool:
        return self.status == AsyncExecutionStatus.FINISHED

    @property
    def t_operation_handle(self) -> ttypes.TOperationHandle:
        """Return the current AsyncExecution as a Thrift TOperationHandle"""

        handle = ttypes.TOperationHandle(
            operationId=ttypes.THandleIdentifier(
                guid=self.query_id.bytes, secret=self.query_secret.bytes
            ),
            operationType=ttypes.TOperationType.EXECUTE_STATEMENT,
            hasResultSet=True,
        )

        return handle

    @property
    def last_sync_timestamp(self) -> Optional[datetime]:
        """The timestamp of the last time self.status was synced with the server"""
        return self._last_sync_timestamp

    @property
    def is_available(self) -> bool:
        """Indicates whether the result of this query can be fetched from a separate thread.

        Only returns False if the query returned its results directly when `execute_async` was called.
        """

        return self._is_available

    @classmethod
    def from_thrift_response(
        cls,
        connection: "Connection",
        thrift_backend: "ThriftBackend",
        resp: ttypes.TExecuteStatementResp,
    ) -> "AsyncExecution":
        """This method is meant to be consumed by `client.py`"""

        return cls(
            connection=connection,
            thrift_backend=thrift_backend,
            query_id=UUID(bytes=resp.operationHandle.operationId.guid),
            query_secret=UUID(bytes=resp.operationHandle.operationId.secret),
            status=_toperationstate_to_ae_status(
                resp.directResults.operationStatus.operationState
            ),
            execute_statement_response=resp,
        )

    @classmethod
    def from_query_id_and_secret(
        cls,
        connection: "Connection",
        thrift_backend: "ThriftBackend",
        query_id: UUID,
        query_secret: UUID,
    ) -> "AsyncExecution":
        """Return a valid AsyncExecution object from a query_id and query_secret.

        Raises an AsyncExecutionException if the query_id:query_secret pair is not found on the server.
        """

        # build a copy of this execution
        ae = cls(
            connection=connection,
            thrift_backend=thrift_backend,
            query_id=query_id,
            query_secret=query_secret,
        )
        # check to make sure this is a valid one
        ae.sync_status()

        return ae
