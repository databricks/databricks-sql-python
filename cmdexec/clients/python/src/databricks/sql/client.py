import base64
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509.oid import NameOID
import datetime
from decimal import Decimal
import logging
import re
import time
from typing import Dict, Tuple, List, Optional, Any

import grpc
import pyarrow

from databricks.sql.errors import OperationalError, InterfaceError, DatabaseError, Error, DataError
from databricks.sql.api import messages_pb2
from databricks.sql.api.sql_cmd_service_pb2_grpc import SqlCommandServiceStub
from databricks.sql import USER_AGENT_NAME, __version__

logger = logging.getLogger(__name__)

DEFAULT_RESULT_BUFFER_SIZE_BYTES = 10485760
DEFAULT_ARRAY_SIZE = 100000

_TIMESTAMP_PATTERN = re.compile(r'(\d+-\d+-\d+ \d+:\d+:\d+(\.\d{,6})?)')


def _parse_timestamp(value):
    if type(value) is datetime.datetime:
        # The cmd exec server will return a datetime.datetime, so no further parsing is needed
        return value
    elif value:
        match = _TIMESTAMP_PATTERN.match(value)
        if match:
            if match.group(2):
                format = '%Y-%m-%d %H:%M:%S.%f'
                # use the pattern to truncate the value
                value = match.group()
            else:
                format = '%Y-%m-%d %H:%M:%S'
            value = datetime.datetime.strptime(value, format)
            return value
        else:
            raise Exception('Cannot convert "{}" into a datetime'.format(value))
    else:
        return None


TYPES_CONVERTER = {
    "decimal": Decimal,
    "timestamp": _parse_timestamp,
}


class Connection:
    def __init__(self,
                 server_hostname: str,
                 http_path: str,
                 access_token: str,
                 metadata: Optional[List[Tuple[str, str]]] = None,
                 **kwargs) -> None:
        """
        Connect to a Databricks SQL endpoint or a Databricks cluster.

        :param server_hostname: Databricks instance host name.
        :param http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
              or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
        :param access_token: Http Bearer access token, e.g. Databricks Personal Access Token.
        :param metadata: An optional list of (k, v) pairs that will be set as Http headers on every request
        """

        # Internal arguments in **kwargs:
        # _user_agent_entry
        #   Tag to add to User-Agent header. For use by partners.
        # _username, _password
        #   Username and password Basic authentication (no official support)
        # _enable_ssl
        #  Connect over HTTP instead of HTTPS
        # _port
        #  Which port to connect to
        # _skip_routing_headers:
        #  Don't set routing headers if set to True (for use when connecting directly to server)
        # _tls_verify_hostname
        #   Set to False (Boolean) to disable SSL hostname verification, but check certificate.
        # _tls_trusted_ca_file
        #   Set to the path of the file containing trusted CA certificates for server certificate
        #   verification. If not provide, uses system truststore.
        # _tls_client_cert_file, _tls_client_cert_key_file
        #   Set client SSL certificate.
        # _session_id
        #   Specify the session id of the connection. For Redash use only.

        self.host = server_hostname
        self.port = kwargs.get("_port", 443)

        if kwargs.get("_username") and kwargs.get("_password"):
            auth_credentials = "{username}:{password}".format(
                username=kwargs.get("_username"), password=kwargs.get("_password")).encode("UTF-8")
            auth_credentials_base64 = base64.standard_b64encode(auth_credentials).decode("UTF-8")
            authorization_header = "Basic {}".format(auth_credentials_base64)
        elif access_token:
            authorization_header = "Bearer {}".format(access_token)
        else:
            raise ValueError("No valid authentication settings.")

        if not kwargs.get("_user_agent_entry"):
            useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)
        else:
            useragent_header = "{}/{} ({})".format(USER_AGENT_NAME, __version__,
                                                   kwargs.get("_user_agent_entry"))

        base_headers = [("Authorization", authorization_header),
                        ("X-Databricks-Sqlgateway-CommandService-Mode", "grpc-thrift"),
                        ("User-Agent", useragent_header)]

        if not kwargs.get("_skip_routing_headers"):
            base_headers.append(self._http_path_to_routing_header(http_path))

        self.base_client = CmdExecBaseHttpClient(
            self.host,
            self.port, (metadata or []) + base_headers,
            enable_ssl=kwargs.get("_enable_ssl", True),
            root_ca_path=kwargs.get("_tls_trusted_ca_file"),
            cert_chain_path=kwargs.get("_tls_client_cert_file"),
            cert_key_path=kwargs.get("_tls_client_cert_key_file"),
            verify_hostname=kwargs.get("_tls_verify_hostname", True))

        open_session_request = messages_pb2.OpenSessionRequest(
            configuration={},
            client_session_id=kwargs.get("_session_id"),
        )

        resp = self.base_client.make_request(self.base_client.stub.OpenSession,
                                             open_session_request)
        self.session_id = resp.session_id
        self.open = True
        logger.info("Successfully opened session " + str(self.session_id.hex()))
        self._cursors = []  # type: List[Cursor]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _http_path_to_routing_header(self, http_path: str) -> Tuple[str, str]:
        cluster_re = r'/?sql/protocolv1/o/\d+/(\d+-\d+-[a-zA-Z0-9]+)'
        endpoint_re = r'/?sql/.*/endpoints/([a-f0-9]+)'
        cluster_id_match = re.search(cluster_re, http_path)
        endpoint_re_match = re.search(endpoint_re, http_path)
        if cluster_id_match:
            return "X-Databricks-Cluster-Id", cluster_id_match.groups()[0]
        elif endpoint_re_match:
            return "X-Databricks-Sql-Endpoint-Id", endpoint_re_match.groups()[0]
        else:
            raise ValueError("Please provide a valid http_path")

    def cursor(self,
               arraysize: int = DEFAULT_ARRAY_SIZE,
               buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES) -> "Cursor":
        """
        Return a new Cursor object using the connection.

        Will throw an Error if the connection has been closed.
        """
        if not self.open:
            raise Error("Cannot create cursor from closed connection")

        cursor = Cursor(self, arraysize=arraysize, result_buffer_size_bytes=buffer_size_bytes)
        self._cursors.append(cursor)
        return cursor

    def close(self) -> None:
        """Close the underlying session and mark all associated cursors as closed."""
        close_session_request = messages_pb2.CloseSessionRequest(session_id=self.session_id)
        self.base_client.make_request(self.base_client.stub.CloseSession, close_session_request)
        self.open = False

        for cursor in self._cursors:
            cursor.close()


class Cursor:
    def __init__(self,
                 connection: Connection,
                 result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
                 arraysize: int = DEFAULT_ARRAY_SIZE) -> None:
        """
        These objects represent a database cursor, which is used to manage the context of a fetch
        operation.

        Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
        visible by other cursors or connections.
        """
        self.connection = connection
        self.rowcount = -1
        self.buffer_size_bytes = result_buffer_size_bytes
        self.active_result_set = None
        self.arraysize = arraysize
        # Note that Cursor closed => active result set closed, but not vice versa
        self.open = True
        self.executing_command_id = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self):
        if self.active_result_set:
            for row in self.active_result_set:
                yield row
        else:
            raise Error("There is no active result set")

    def _response_to_result_set(self, execute_command_response, status):
        command_id = execute_command_response.command_id
        arrow_results = execute_command_response.results.arrow_ipc_stream
        schema_message = execute_command_response.results.metadata \
                         and execute_command_response.results.metadata.schema
        has_been_closed_server_side = execute_command_response.closed
        has_more_rows = execute_command_response.results.has_more_rows
        num_valid_rows = execute_command_response.results.num_valid_rows
        if arrow_results:
            arrow_results_and_n_rows = (arrow_results, num_valid_rows)
        else:
            arrow_results_and_n_rows = None

        return ResultSet(self.connection, command_id, status, has_been_closed_server_side,
                         has_more_rows, arrow_results_and_n_rows, schema_message,
                         self.buffer_size_bytes)

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
        if status == messages_pb2.COMMAND_STATE_ERROR:
            raise DatabaseError(
                "Command %s failed with error message %s" % (command_id, resp.status.error_message))
        elif status == messages_pb2.COMMAND_STATE_CLOSED:
            raise DatabaseError("Command %s closed before results could be fetched" % command_id)

    def _poll_for_state(self, command_id):
        get_status_request = messages_pb2.GetCommandStatusRequest(command_id=command_id)

        resp = self.connection.base_client.make_request(
            self.connection.base_client.stub.GetCommandStatus, get_status_request)

        logger.info("Status for command %s is: %s" % (command_id, resp.status))
        return resp

    def _wait_until_command_done(self, command_id, initial_status):
        status = initial_status
        while status in [messages_pb2.COMMAND_STATE_PENDING, messages_pb2.COMMAND_STATE_RUNNING]:
            resp = self._poll_for_state(command_id)
            status = resp.status.state
            self._check_response_for_error(resp, command_id)

            # TODO: Remove this sleep once we have long-polling on the server (SC-77653)
            time.sleep(0.1)
        return status

    def execute(self,
                operation: str,
                query_params: Optional[Dict[str, str]] = None,
                metadata: Optional[Dict[str, str]] = None) -> "Cursor":
        """
        Execute a query and wait for execution to complete.

        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()
        return self._create_result_set_from_command(
            sql_command=messages_pb2.SqlCommand(command=[operation]))

    def tables(self,
               catalog_name: Optional[str] = None,
               schema_name: Optional[str] = None,
               table_name: Optional[str] = None,
               table_types: List[str] = []) -> "Cursor":
        """
        Get tables corresponding to the catalog_name, schema_name and table_name.

        Names can contain % wildcards.
        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()

        get_tables_command = messages_pb2.GetTablesCommand(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            table_types=table_types)

        return self._create_result_set_from_command(get_tables_command=get_tables_command)

    def columns(self,
                catalog_name: Optional[str] = None,
                schema_name: Optional[str] = None,
                table_name: Optional[str] = None,
                column_name: Optional[str] = None) -> "Cursor":
        """
        Get columns corresponding to the catalog_name, schema_name, table_name and column_name.

        Names can contain % wildcards.
        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()

        get_columns_command = messages_pb2.GetColumnsCommand(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name)

        return self._create_result_set_from_command(get_columns_command=get_columns_command)

    def schemas(self, catalog_name: Optional[str] = None,
                schema_name: Optional[str] = None) -> "Cursor":
        """
        Get columns corresponding to the catalog_name and schema_name.

        Names can contain % wildcards.
        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()

        get_schemas_command = messages_pb2.GetSchemasCommand(
            catalog_name=catalog_name, schema_name=schema_name)

        return self._create_result_set_from_command(get_schemas_command=get_schemas_command)

    def _create_result_set_from_command(self, **command_arg):
        # Create execute command request
        execute_command_request = messages_pb2.ExecuteCommandRequest(
            session_id=self.connection.session_id,
            client_command_id=None,
            conf_overlay=None,
            row_limit=None,
            result_options=messages_pb2.CommandResultOptions(
                max_rows=self.arraysize,
                max_bytes=self.buffer_size_bytes,
                include_metadata=True,
            ),
            **command_arg)

        execute_command_response = self.connection.base_client.make_request(
            self.connection.base_client.stub.ExecuteCommand, execute_command_request)
        initial_status = execute_command_response.status.state
        command_id = execute_command_response.command_id

        try:
            self.executing_command_id = command_id
            self._check_response_for_error(execute_command_response, command_id)
            final_status = self._wait_until_command_done(command_id, initial_status)
            self.active_result_set = self._response_to_result_set(execute_command_response,
                                                                  final_status)
        finally:
            self.executing_command_id = None

        return self

    def fetchall(self) -> List[Tuple]:
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences.

        A databricks.sql.errors.Error (or subclass) exception is raised if the previous call to
        execute did not produce any result set or no call was issued yet.
        """
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchall()
        else:
            raise Error("There is no active result set")

    def fetchone(self) -> Tuple:
        """
        Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available.

        An databricks.sql.errors.Error (or subclass) exception is raised if the previous call to
        execute did not produce any result set or no call was issued yet.
        """
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchone()
        else:
            raise Error("There is no active result set")

    def fetchmany(self, n_rows: int) -> List[Tuple]:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples).

        An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter n_rows. If it is not
        given, the cursor's arraysize determines the number of rows to be fetched. The method
        should try to fetch as many rows as indicated by the size parameter. If this is not
        possible due to the specified number of rows not being available, fewer rows may be
        returned.

        A databricks.sql.errors.Error (or subclass) exception is raised if the previous call
        to execute did not produce any result set or no call was issued yet.
        """
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchmany(n_rows)
        else:
            raise Error("There is no active result set")

    def cancel(self) -> None:
        """
        Cancel a running command.

        The command should be closed to free resources from the server.
        This method can be called from another thread.
        """
        command_id = self.executing_command_id

        if command_id is not None:
            logger.info("Canceling command %s" % command_id)
            cancel_command_request = messages_pb2.CancelCommandRequest(command_id=command_id)

            self.connection.base_client.make_request(self.connection.base_client.stub.CancelCommand,
                                                     cancel_command_request)
        else:
            raise Error("There is no executing command to cancel")

    def close(self) -> None:
        """Close cursor"""
        self.open = False
        if self.active_result_set:
            self._close_and_clear_active_result_set()

    @property
    def description(self) -> Optional[List[Tuple]]:
        """
        This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the execute method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects.
        """
        if self.active_result_set:
            return self.active_result_set.description
        else:
            return None


class ResultSet:
    def __init__(self,
                 connection: Connection,
                 command_id,
                 status,
                 has_been_closed_server_side: bool,
                 has_more_rows: bool,
                 arrow_ipc_stream_with_n_rows: Optional[Tuple[Any, int]] = None,
                 schema_message=None,
                 result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
                 arraysize: int = 10000):
        """
        A ResultSet manages the results of a single command.

        :param connection: The parent connection that was used to execute this command
        :param command_id: The id of the command
        :param status: The status of the command
        :param has_been_closed_server_side: True iff the command has been closed already by the
        server (this is the case if the fast path was taken and all results returned)
        :param has_more_rows: True iff there are more rows to fetch on the server
        :param arrow_ipc_stream_with_n_rows: An optional (Arrow stream, int) pair. Returned if
        the fast path was taken on the server. The int in the tuple is how many rows are actually
        considered valid
        :param schema_message: The gRPC message containing the schema
        :param result_buffer_size_bytes: The size (in bytes) of the internal buffer + max fetch
        amount :param arraysize: The max number of rows to fetch at a time (PEP-249)
        """
        self.connection = connection
        self.command_id = command_id
        self.status = status
        self.has_been_closed_server_side = has_been_closed_server_side
        self.has_more_rows = has_more_rows
        self.buffer_size_bytes = result_buffer_size_bytes
        self._row_index = 0
        self.description = None
        self.arraysize = arraysize

        assert (self.status not in [
            messages_pb2.COMMAND_STATE_PENDING, messages_pb2.COMMAND_STATE_RUNNING
        ])

        if arrow_ipc_stream_with_n_rows:
            # In this case the server has taken the fast path and returned an initial batch of
            # results
            arrow_ipc_stream, num_valid_rows = arrow_ipc_stream_with_n_rows
            self.results = ArrowQueue(
                pyarrow.ipc.open_stream(arrow_ipc_stream).read_all(), num_valid_rows, 0)
            self.description = self._get_schema_description(schema_message)
        else:
            # In this case, there are results waiting on the server so we fetch now for simplicity
            self._fill_results_buffer()

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row:
                yield row
            else:
                break

    def _fetch_and_deserialize_results(self):
        fetch_results_request = messages_pb2.FetchCommandResultsRequest(
            command_id=self.command_id,
            options=messages_pb2.CommandResultOptions(
                max_bytes=self.buffer_size_bytes,
                max_rows=self.arraysize,
                row_offset=self._row_index,
                include_metadata=True,
            ))

        result_message = self.connection.base_client.make_request(
            self.connection.base_client.stub.FetchCommandResults, fetch_results_request).results
        num_valid_rows = result_message.num_valid_rows
        arrow_table = pyarrow.ipc.open_stream(result_message.arrow_ipc_stream).read_all()
        results = ArrowQueue(arrow_table, num_valid_rows,
                             self._row_index - result_message.start_row_offset)
        description = self._get_schema_description(result_message.metadata.schema)
        return results, result_message.has_more_rows, description

    def _fill_results_buffer(self):
        if self.status == messages_pb2.COMMAND_STATE_CLOSED:
            raise Error("Can't fetch results on closed command %s" % self.command_id)
        elif self.status == messages_pb2.COMMAND_STATE_ERROR:
            raise DatabaseError("Command %s failed" % self.command_id)
        else:
            results, has_more_rows, description = self._fetch_and_deserialize_results()
            self.results = results
            self.has_more_rows = has_more_rows
            self.description = description

    @staticmethod
    def parse_type(type_, value):
        converter = TYPES_CONVERTER.get(type_)
        if converter:
            return converter(value)
        else:
            return value

    def _convert_arrow_table(self, table):
        n_rows, _ = table.shape
        list_repr = [[
            self.parse_type(self.description[col_index][1], col[row_index].as_py())
            for col_index, col in enumerate(table.itercolumns())
        ] for row_index in range(n_rows)]
        return list_repr

    def fetchmany_arrow(self, n_rows: int) -> pyarrow.Table:
        """
        Fetch the next set of rows of a query result, returning a PyArrow table.

        An empty sequence is returned when no more rows are available.
        """
        # TODO: Make efficient with less copying
        if n_rows < 0:
            raise ValueError("n_rows argument for fetchmany is %s but must be >= 0", n_rows)
        results = self.results.next_n_rows(n_rows)
        n_remaining_rows = n_rows - results.num_rows
        self._row_index += results.num_rows

        while n_remaining_rows > 0 and not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            results = pyarrow.concat_tables([results, partial_results])
            n_remaining_rows -= partial_results.num_rows
            self._row_index += partial_results.num_rows

        return results

    def fetchall_arrow(self) -> pyarrow.Table:
        """Fetch all (remaining) rows of a query result, returning them as a PyArrow table."""
        results = self.results.remaining_rows()
        self._row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            results = pyarrow.concat_tables([results, partial_results])
            self._row_index += partial_results.num_rows

        return results

    def fetchone(self) -> Optional[Tuple]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        """
        res = self._convert_arrow_table(self.fetchmany_arrow(1))
        if len(res) > 0:
            return res[0]
        else:
            return None

    def fetchall(self) -> List[Tuple]:
        """
        Fetch all (remaining) rows of a query result, returning them as a list of lists.
        """
        return self._convert_arrow_table(self.fetchall_arrow())

    def fetchmany(self, n_rows: int) -> List[Tuple]:
        """
        Fetch the next set of rows of a query result, returning a list of lists.

        An empty sequence is returned when no more rows are available.
        """
        return self._convert_arrow_table(self.fetchmany_arrow(n_rows))

    def close(self) -> None:
        """
        Close the cursor.

        If the connection has not been closed, and the cursor has not already
        been closed on the server for some other reason, issue a request to the server to close it.
        """
        try:
            if self.status != messages_pb2.COMMAND_STATE_CLOSED and not self.has_been_closed_server_side \
              and self.connection.open:
                close_command_request = messages_pb2.CloseCommandRequest(command_id=self.command_id)
                self.connection.base_client.make_request(
                    self.connection.base_client.stub.CloseCommand, close_command_request)
        finally:
            self.has_been_closed_server_side = True
            self.status = messages_pb2.COMMAND_STATE_CLOSED

    @staticmethod
    def _get_schema_description(table_schema_message):
        """
        Takes a TableSchema message and returns a description 7-tuple as specified by PEP-249
        """

        def map_col_type(type_):
            if type_.startswith('decimal'):
                return 'decimal'
            else:
                return type_

        return [(column.name, map_col_type(column.datatype), None, None, None, None, None)
                for column in table_schema_message.columns]


class ArrowQueue:
    def __init__(self, arrow_table: pyarrow.Table, n_valid_rows: int, start_row_index: int):
        """
        A queue-like wrapper over an Arrow table

        :param arrow_table: The Arrow table from which we want to take rows
        :param n_valid_rows: The index of the last valid row in the table
        :param start_row_index: The first row in the table we should start fetching from
        """
        self.cur_row_index = start_row_index
        self.arrow_table = arrow_table
        self.n_valid_rows = n_valid_rows

    def next_n_rows(self, num_rows: int) -> pyarrow.Table:
        """Get upto the next n rows of the Arrow dataframe"""
        length = min(num_rows, self.n_valid_rows - self.cur_row_index)
        slice = self.arrow_table.slice(self.cur_row_index, length)
        self.cur_row_index += slice.num_rows
        return slice

    def remaining_rows(self) -> pyarrow.Table:
        slice = self.arrow_table.slice(self.cur_row_index, self.n_valid_rows - self.cur_row_index)
        self.cur_row_index += slice.num_rows
        return slice


class CmdExecBaseHttpClient:
    """A thin wrapper around a gRPC channel that takes cares of headers etc."""

    def __init__(self,
                 host: str,
                 port: int,
                 http_headers: List[Tuple[str, str]],
                 enable_ssl: bool = True,
                 root_ca_path: Optional[str] = None,
                 cert_chain_path: Optional[str] = None,
                 cert_key_path: Optional[str] = None,
                 verify_hostname: bool = True):
        self.host_url = host + ":" + str(port)
        self.http_headers = [(k.lower(), str(v)) for (k, v) in http_headers]
        if enable_ssl:
            if root_ca_path:
                try:
                    with open(root_ca_path, 'rb') as f:
                        root_ca = f.read()  # type: Optional[bytes]
                except OSError as e:
                    raise OperationalError(
                        "Error while trying to read root SSL certificate %r:" % root_ca_path, e)
            else:
                root_ca = None

            if cert_chain_path:
                try:
                    with open(cert_chain_path, 'rb') as f:
                        cert_chain = f.read()  # type: Optional[bytes]
                except OSError as e:
                    raise OperationalError(
                        "Error while trying to read SSL certificate chain %r:" % cert_chain, e)
            else:
                cert_chain = None

            if cert_key_path:
                try:
                    with open(cert_key_path, 'rb') as f:
                        cert_key = f.read()  # type: Optional[bytes]
                except OSError as e:
                    raise OperationalError(
                        "Error while trying to read SSL certificate key %r:" % cert_key_path, e)
            else:
                cert_key = None

            if not verify_hostname and root_ca:
                # gRPC doesn't have a flag that lets us completely disable the cn name check,
                # so we just set the target name override so they match.
                cert_info = x509.load_pem_x509_certificate(root_ca, default_backend())
                cn = cert_info.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
                target_name_override_opt = [('grpc.ssl_target_name_override', cn)]
            else:
                target_name_override_opt = []

            self.channel = grpc.secure_channel(
                self.host_url,
                options=[('grpc.max_receive_message_length', -1)] + target_name_override_opt,
                credentials=grpc.ssl_channel_credentials(
                    root_certificates=root_ca, certificate_chain=cert_chain, private_key=cert_key))
        else:
            self.channel = grpc.insecure_channel(
                self.host_url,
                options=[('grpc.max_receive_message_length', -1)],
            )
        self.stub = SqlCommandServiceStub(self.channel)

    def make_request(self, method, request):
        try:
            response = method(request, metadata=self.http_headers)
            logger.info("Received message: %s", response)
            return response
        except grpc.RpcError as error:
            logger.error("Received error during gRPC request: %s", error)
            raise OperationalError("Error during gRPC request", error)
