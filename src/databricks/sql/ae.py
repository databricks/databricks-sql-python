from enum import Enum
from typing import Optional, Union, TYPE_CHECKING
from databricks.sql.results import ResultSet


if TYPE_CHECKING:
    from databricks.sql.thrift_backend import ThriftBackend
    from databricks.sql.client import Connection
    from databricks.sql.results import ResultSet


from uuid import UUID

from databricks.sql.thrift_api.TCLIService import ttypes


class AsyncExecutionStatus(Enum):
    """An enum that represents the status of an async execution"""

    PENDING = 0
    RUNNING = 1
    FINISHED = 2
    CANCELED = 3
    FETCHED = 4
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
    A class that represents an async execution of a query. Exposes just two methods:
    get_result_or_status and cancel
    """

    _connection: "Connection"
    _result_set: Optional["ResultSet"]
    _execute_statement_response: Optional[ttypes.TExecuteStatementResp]

    def __init__(
        self,
        connection: "Connection",
        query_id: UUID,
        status: AsyncExecutionStatus,
        execute_statement_response: Optional[ttypes.TExecuteStatementResp] = None,
    ):
        self._connection = connection
        self._execute_statement_response = execute_statement_response
        self.query_id = query_id
        self.status = status

    status: AsyncExecutionStatus
    query_id: UUID

    def get_result_or_status(self) -> Union["ResultSet", AsyncExecutionStatus]:
        """Get the result of the async execution. If execution has not completed, return False."""

        if self.status == AsyncExecutionStatus.FINISHED:
            self._thrift_fetch_result()
        if self.status == AsyncExecutionStatus.FETCHED:
            return self._result_set
        else:
            self._thrift_get_operation_status()
            return self.status

    def cancel(self) -> None:
        """Cancel the query"""
        self._thrift_cancel_operation()

    def _thrift_cancel_operation(self) -> None:
        """Execute TCancelOperation"""

        _output = self._connection.thrift_backend.cancel_command(self.query_id)
        self.status = AsyncExecutionStatus.CANCELED

    def _thrift_get_operation_status(self) -> None:
        """Execute GetOperationStatusReq and map thrift execution status to DbsqlAsyncExecutionStatus"""

        _output = self._connection.thrift_backend._poll_for_status(self.query_id)
        self.status = _toperationstate_to_ae_status(_output)

    def _thrift_fetch_result(self) -> None:
        """Execute TFetchResultReq and store the result"""

        # A cursor is required here to hook into the thrift_backend result fetching API
        # TODO: need to rewrite this to use a generic result fetching API so we can
        # support JSON and Thrift binary result formats in addition to arrow.

        # in the case of direct results this creates a second cursor...how can I avoid that?
        with self._connection.cursor() as cursor:
            er = self._connection.thrift_backend._handle_execute_response(
                self._execute_statement_response, cursor
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
