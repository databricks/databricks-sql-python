from enum import Enum
from typing import Optional, Union, TYPE_CHECKING


if TYPE_CHECKING:
    from databricks.sql.thrift_backend import ThriftBackend
    from databricks.sql.client import Connection
    from databricks.sql.client import ResultSet


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

    def __init__(self, connection: "Connection", query_id: UUID, status: AsyncExecutionStatus):
        self._connection = connection
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
        self._result_set = self._thrift_backend.fetch_results(self.query_id)[0]
        self.status == AsyncExecutionStatus.FETCHED