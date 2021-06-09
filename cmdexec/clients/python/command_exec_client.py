import logging
from typing import Dict, Tuple, List
from collections import deque

import grpc
import pyarrow

import cmdexec.clients.python.sql_command_service_pb2 as command_pb2
from cmdexec.clients.python.sql_command_service_pb2_grpc import SqlCommandServiceStub
from cmdexec.clients.python.errors import OperationalError, InterfaceError, DatabaseError, Error

import time

logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE_ROWS = 1000


def connect(**kwargs):
    return Connection(**kwargs)


class Connection:
    def __init__(self, **kwargs):
        try:
            self.host = kwargs["HOST"]
            self.port = kwargs["PORT"]
        except KeyError:
            raise InterfaceError("Please include arguments HOST and PORT in kwargs for Connection")

        self.base_client = CmdExecBaseHttpClient(self.host, self.port, kwargs.get("metadata", []))
        open_session_request = command_pb2.OpenSessionRequest(
            configuration={},
            client_session_id=None,
            session_info_fields=None,
        )

        resp = self.base_client.make_request(self.base_client.stub.OpenSession,
                                             open_session_request)
        self.session_id = resp.id
        self.open = True
        logger.info("Successfully opened session " + str(self.session_id.hex()))
        self._cursors = []

    def cursor(self, buffer_size_rows=DEFAULT_BUFFER_SIZE_ROWS):
        if not self.open:
            raise Error("Cannot create cursor from closed connection")
        cursor = Cursor(self, buffer_size_rows)
        self._cursors.append(cursor)
        return cursor

    def close(self):
        close_session_request = command_pb2.CloseSessionRequest(id=self.session_id)
        self.base_client.make_request(self.base_client.stub.CloseSession, close_session_request)
        self.open = False

        for cursor in self._cursors:
            cursor.close()


class Cursor:
    def __init__(self, connection, buffer_size_rows=DEFAULT_BUFFER_SIZE_ROWS):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self.buffer_size_rows = buffer_size_rows
        self.active_result_set = None
        # Note that Cursor closed => active result set closed, but not vice versa
        self.open = True

    def _response_to_result_set(self, execute_command_response, status):
        command_id = execute_command_response.command_id
        arrow_results = execute_command_response.results.arrow_ipc_stream
        has_been_closed_server_side = execute_command_response.closed
        number_of_valid_rows = execute_command_response.results.number_of_valid_rows

        return ResultSet(self.connection, command_id, status, has_been_closed_server_side,
                         arrow_results, number_of_valid_rows, self.buffer_size_rows)

    def _close_and_clear_active_result_set(self):
        try:
            if self.active_result_set:
                self.active_result_set.close()
        finally:
            self.active_result_set = None

    def _check_not_closed(self):
        if not self.open:
            raise Error("Attempting operation on closed cursor")

    def _check_response_for_error(self, resp, command_id):
        status = resp.status.state
        if status == command_pb2.ERROR:
            raise DatabaseError(
                "Command %s failed with error message %s" % (command_id, resp.status.error_message))
        elif status == command_pb2.CLOSED:
            raise DatabaseError("Command %s closed before results could be fetched" % command_id)

    def _poll_for_state(self, command_id):
        get_status_request = command_pb2.GetCommandStatusRequest(id=command_id)

        resp = self.connection.base_client.make_request(
            self.connection.base_client.stub.GetCommandStatus, get_status_request)

        logger.info("Status for command %s is: %s" % (command_id, resp.status))
        return resp

    def _wait_until_command_done(self, command_id, initial_status):
        status = initial_status
        print("initial status: %s" % status)
        while status in [command_pb2.PENDING, command_pb2.RUNNING]:
            resp = self._poll_for_state(command_id)
            status = resp.status.state
            self._check_response_for_error(resp, command_id)
            print("status is: %s" % status)

            # TODO: Remove this sleep once we have long-polling on the server (SC-77653)
            time.sleep(1)
        return status

    def execute(self, operation, query_params=None, metadata=None):
        self._check_not_closed()
        self._close_and_clear_active_result_set()

        # Execute the command
        execute_command_request = command_pb2.ExecuteCommandRequest(
            session_id=self.connection.session_id,
            client_command_id=None,
            command=operation,
            conf_overlay=None,
            row_limit=None,
            result_options=None,
        )

        execute_command_response = self.connection.base_client.make_request(
            self.connection.base_client.stub.ExecuteCommand, execute_command_request)
        initial_status = execute_command_response.status.state
        command_id = execute_command_response.command_id

        self._check_response_for_error(execute_command_response, command_id)
        final_status = self._wait_until_command_done(command_id, initial_status)
        self.active_result_set = self._response_to_result_set(execute_command_response,
                                                              final_status)

        return self

    def fetchall(self):
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchall()
        else:
            raise Error("There is no active result set")

    def fetchone(self):
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchone()
        else:
            raise Error("There is no active result set")

    def fetchmany(self, n_rows):
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchmany(n_rows)
        else:
            raise Error("There is no active result set")

    def close(self):
        self.open = False
        if self.active_result_set:
            self._close_and_clear_active_result_set()


class ResultSet:
    def __init__(self,
                 connection,
                 command_id,
                 status,
                 has_been_closed_server_side,
                 arrow_ipc_stream=None,
                 number_of_valid_rows=None,
                 buffer_size_rows=DEFAULT_BUFFER_SIZE_ROWS):
        self.connection = connection
        self.command_id = command_id
        self.status = status
        self.has_been_closed_server_side = has_been_closed_server_side
        self.buffer_size_rows = buffer_size_rows

        assert (self.status not in [command_pb2.PENDING, command_pb2.RUNNING])

        if arrow_ipc_stream:
            self.results = deque(
                self._deserialize_arrow_ipc_stream(arrow_ipc_stream)[:number_of_valid_rows])
            self.has_more_rows = False
        else:
            self.results = deque()
            self.has_more_rows = True

    def _deserialize_arrow_ipc_stream(self, ipc_stream):
        # TODO: Proper results deserialization, taking into account the logical schema, convert
        #  via pd df for efficiency (SC-77871)
        pyarrow_table = pyarrow.ipc.open_stream(ipc_stream).read_all()
        dict_repr = pyarrow_table.to_pydict()
        n_rows, n_cols = pyarrow_table.shape
        list_repr = [[col[i] for col in dict_repr.values()] for i in range(n_rows)]
        return list_repr

    def _fetch_and_deserialize_results(self):
        fetch_results_request = command_pb2.FetchCommandResultsRequest(
            id=self.command_id,
            options=command_pb2.CommandResultOptions(
                max_rows=self.buffer_size_rows,
                include_metadata=True,
            ))

        result_message = self.connection.base_client.make_request(
            self.connection.base_client.stub.FetchCommandResults, fetch_results_request).results
        number_of_valid_rows = result_message.number_of_valid_rows
        # TODO: Make efficient with less copying (https://databricks.atlassian.net/browse/SC-77868)
        results = deque(
            self._deserialize_arrow_ipc_stream(
                result_message.arrow_ipc_stream)[:number_of_valid_rows])
        return results, result_message.has_more_rows

    def _fill_results_buffer(self):
        if self.status == command_pb2.CLOSED:
            raise Error("Can't fetch results on closed command %s" % self.command_id)
        elif self.status == command_pb2.ERROR:
            raise DatabaseError("Command %s failed" % self.command_id)
        else:
            results, has_more_rows = self._fetch_and_deserialize_results()
            self.results = results
            if not has_more_rows:
                self.has_more_rows = False

    def _take_n_from_deque(self, deque, n):
        arr = []
        for _ in range(n):
            try:
                arr.append(deque.popleft())
            except IndexError:
                break
        return arr

    def fetchmany(self, n_rows):
        # TODO: Make efficient with less copying
        if n_rows < 0:
            raise ValueError("n_rows argument for fetchmany is %s but must be >= 0", n_rows)
        results = self._take_n_from_deque(self.results, n_rows)
        n_remaining_rows = n_rows - len(results)

        while n_remaining_rows > 0 and not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self._take_n_from_deque(self.results, n_remaining_rows)
            results += partial_results
            n_remaining_rows -= len(partial_results)

        return results

    def fetchone(self):
        return self.fetchmany(1)

    def fetchall(self):
        results = []
        while True:
            partial_results = self.fetchmany(self.buffer_size_rows)
            # TODO: What's the optimal sequence of sizes to fetch?
            results += partial_results

            if len(partial_results) == 0:
                break

        return results

    def close(self):
        try:
            if self.status != command_pb2.CLOSED and not self.has_been_closed_server_side \
              and self.connection.open:
                close_command_request = command_pb2.CloseCommandRequest(id=self.command_id)
                self.connection.base_client.make_request(
                    self.connection.base_client.stub.CloseCommand, close_command_request)
        finally:
            self.has_been_closed_server_side = True
            self.status = command_pb2.CLOSED


class CmdExecBaseHttpClient:
    """
    A thin wrapper around a gRPC channel that takes cares of headers etc.
    """

    def __init__(self, host: str, port: int, http_headers: List[Tuple[str, str]]):
        self.host_url = host + ":" + str(port)
        self.http_headers = [(k.lower(), v) for (k, v) in http_headers]
        self.channel = grpc.insecure_channel(self.host_url)
        self.stub = SqlCommandServiceStub(self.channel)

    def make_request(self, method, request):
        try:
            response = method(request, metadata=self.http_headers)
            logger.info("Received message: %s", response)
            return response
        except grpc.RpcError as error:
            logger.error("Received error during gRPC request: %s", error)
            raise OperationalError("Error during gRPC request", error)
