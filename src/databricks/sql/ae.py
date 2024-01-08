from enum import Enum
from typing import Optional, Union, TYPE_CHECKING
from databricks.sql.results import ResultSet

from dataclasses import dataclass

if TYPE_CHECKING:
    from databricks.sql.thrift_backend import ThriftBackend
    from databricks.sql.client import Connection
    from databricks.sql.results import ResultSet


from uuid import UUID

from databricks.sql.thrift_api.TCLIService import ttypes


class AsyncExecutionException(Exception):
    pass


@dataclass
class FakeCursor:
    active_op_handle: Optional[ttypes.TOperationHandle]


class AsyncExecutionStatus(Enum):
    """An enum that represents the status of an async execution"""

    PENDING = 0
    RUNNING = 1
    FINISHED = 2
    CANCELED = 3
    FETCHED = 4

    # todo: when is this ever evaluated?
    ABORTED = 5


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
    A class that represents an async execution of a query.

    AsyncExecutions are effectively connectionless. But because thrift_backend is entangled
    with client.py, the AsyncExecution needs access to both a Connection and a ThriftBackend

    This will need to be refactored for cleanliness in the future.
    """

    _connection: "Connection"
    _thrift_backend: "ThriftBackend"
    _result_set: Optional["ResultSet"]
    _execute_statement_response: Optional[ttypes.TExecuteStatementResp]

    def __init__(
        self,
        thrift_backend: "ThriftBackend",
        connection: "Connection",
        query_id: UUID,
        query_secret: UUID,
        status: AsyncExecutionStatus,
        execute_statement_response: Optional[ttypes.TExecuteStatementResp] = None,
    ):
        self._connection = connection
        self._thrift_backend = thrift_backend
        self._execute_statement_response = execute_statement_response
        self.query_id = query_id
        self.query_secret = query_secret
        self.status = status

    status: AsyncExecutionStatus
    query_id: UUID

    def get_result(self) -> "ResultSet":
        """Get a result set for this async execution

        Raises an exception if the query is still running or has been canceled.
        """

        if self.status == AsyncExecutionStatus.CANCELED:
            raise AsyncExecutionException("Query was canceled: %s" % self.query_id)
        if self.is_running:
            raise AsyncExecutionException("Query is still running: %s" % self.query_id)
        if self.status == AsyncExecutionStatus.FINISHED:
            self._thrift_fetch_result()
        if self.status == AsyncExecutionStatus.FETCHED:
            return self._result_set

    def poll_for_status(self) -> None:
        """Check the thrift server for the status of this operation and set self.status

        This will result in an error if the operaiton has been canceled or aborted at the server"""
        self._thrift_get_operation_status()

    def cancel(self) -> None:
        """Cancel the query"""
        self._thrift_cancel_operation()

    def _thrift_cancel_operation(self) -> None:
        """Execute TCancelOperation"""

        _output = self._thrift_backend.async_cancel_command(self.t_operation_handle)
        self.status = AsyncExecutionStatus.CANCELED

    def _thrift_get_operation_status(self) -> None:
        """Execute GetOperationStatusReq and map thrift execution status to DbsqlAsyncExecutionStatus"""

        _output = self._thrift_backend._poll_for_status(self.t_operation_handle)
        self.status = _toperationstate_to_ae_status(_output)

    def _thrift_fetch_result(self) -> None:
        """Execute TFetchResultReq and store the result"""

        # A cursor is required here to hook into the thrift_backend result fetching API
        # TODO: need to rewrite this to use a generic result fetching API so we can
        # support JSON and Thrift binary result formats in addition to arrow.

        # in the case of direct results this creates a second cursor...how can I avoid that?

        er = self._thrift_backend._handle_execute_response(
            self._execute_statement_response, FakeCursor(None)
        )

        self._result_set = ResultSet(
            connection=self._connection,
            execute_response=er,
            thrift_backend=self._connection.thrift_backend,
        )

        self.status = AsyncExecutionStatus.FETCHED

    @property
    def is_running(self) -> bool:
        return self.status in [
            AsyncExecutionStatus.RUNNING,
            AsyncExecutionStatus.PENDING,
        ]

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
