import base64
import datetime
from decimal import Decimal
import logging
import re
from typing import Dict, Tuple, List, Optional, Any

import pyarrow

from databricks.sql import USER_AGENT_NAME, __version__
from databricks.sql import *
from databricks.sql.thrift_backend import ThriftBackend
from databricks.sql.utils import ExecuteResponse

logger = logging.getLogger(__name__)

DEFAULT_RESULT_BUFFER_SIZE_BYTES = 10485760
DEFAULT_ARRAY_SIZE = 100000


class Connection:
    def __init__(self,
                 server_hostname: str,
                 http_path: str,
                 access_token: str,
                 metadata: Optional[List[Tuple[str, str]]] = None,
                 session_configuration: Dict[str, Any] = None,
                 **kwargs) -> None:
        """
        Connect to a Databricks SQL endpoint or a Databricks cluster.

        :param server_hostname: Databricks instance host name.
        :param http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
              or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
        :param access_token: Http Bearer access token, e.g. Databricks Personal Access Token.
        :param metadata: An optional list of (k, v) pairs that will be set as Http headers on every request
        :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None. 
               Execute the SQL command `SET -v` to get a full list of available commands.
        """

        # Internal arguments in **kwargs:
        # _user_agent_entry
        #   Tag to add to User-Agent header. For use by partners.
        # _username, _password
        #   Username and password Basic authentication (no official support)
        # _use_cert_as_auth
        #  Use a TLS cert instead of a token or username / password (internal use only)
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
        # _retry_stop_after_attempts_count
        #  The maximum number of attempts during a request retry sequence (defaults to 24)
        # _socket_timeout
        #  The timeout in seconds for socket send, recv and connect operations. Defaults to None for
        #  no timeout. Should be a positive float or integer.

        self.host = server_hostname
        self.port = kwargs.get("_port", 443)

        authorization_header = []
        if kwargs.get("_username") and kwargs.get("_password"):
            auth_credentials = "{username}:{password}".format(
                username=kwargs.get("_username"), password=kwargs.get("_password")).encode("UTF-8")
            auth_credentials_base64 = base64.standard_b64encode(auth_credentials).decode("UTF-8")
            authorization_header = [("Authorization", "Basic {}".format(auth_credentials_base64))]
        elif access_token:
            authorization_header = [("Authorization", "Bearer {}".format(access_token))]
        elif not (kwargs.get("_use_cert_as_auth") and kwargs.get("_tls_client_cert_file")):
            raise ValueError("No valid authentication settings. Please provide an access token.")

        if not kwargs.get("_user_agent_entry"):
            useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)
        else:
            useragent_header = "{}/{} ({})".format(USER_AGENT_NAME, __version__,
                                                   kwargs.get("_user_agent_entry"))

        base_headers = [("User-Agent", useragent_header)] + authorization_header
        self.thrift_backend = ThriftBackend(self.host, self.port, http_path,
                                            (metadata or []) + base_headers, **kwargs)

        self._session_handle = self.thrift_backend.open_session(session_configuration)
        self.open = True
        logger.info("Successfully opened session " + str(self.get_session_id()))
        self._cursors = []  # type: List[Cursor]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_session_id(self):
        return self.thrift_backend.handle_to_id(self._session_handle)

    def cursor(self,
               arraysize: int = DEFAULT_ARRAY_SIZE,
               buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES) -> "Cursor":
        """
        Return a new Cursor object using the connection.

        Will throw an Error if the connection has been closed.
        """
        if not self.open:
            raise Error("Cannot create cursor from closed connection")

        cursor = Cursor(
            self,
            self.thrift_backend,
            arraysize=arraysize,
            result_buffer_size_bytes=buffer_size_bytes)
        self._cursors.append(cursor)
        return cursor

    def close(self) -> None:
        """Close the underlying session and mark all associated cursors as closed."""
        self.thrift_backend.close_session(self._session_handle)
        self.open = False

        for cursor in self._cursors:
            cursor.close()


class Cursor:
    def __init__(self,
                 connection: Connection,
                 thrift_backend: ThriftBackend,
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
        self.thrift_backend = thrift_backend
        self.active_op_handle = None

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

    def _close_and_clear_active_result_set(self):
        try:
            if self.active_result_set:
                self.active_result_set.close()
        finally:
            self.active_result_set = None

    def _check_not_closed(self):
        if not self.open:
            raise Error("Attempting operation on closed cursor")

    def execute(self,
                operation: str,
                query_params: Optional[Dict[str, str]] = None,
                metadata: Optional[Dict[str, str]] = None) -> "Cursor":
        """
        Execute a query and wait for execution to complete.

        :returns self
        """
        if query_params is None:
            sql = operation
        else:
            # TODO(https://databricks.atlassian.net/browse/SC-88829) before public release
            logger.error("query param substitution currently un-implemented")
            sql = operation

        self._check_not_closed()
        self._close_and_clear_active_result_set()
        execute_response = self.thrift_backend.execute_command(
            operation=sql,
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self)
        self.active_result_set = ResultSet(self.connection, execute_response, self.thrift_backend,
                                           self.buffer_size_bytes, self.arraysize)
        return self

    def catalogs(self) -> "Cursor":
        """
        Get all available catalogs.

        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()
        execute_response = self.thrift_backend.get_catalogs(
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self)
        self.active_result_set = ResultSet(self.connection, execute_response, self.thrift_backend,
                                           self.buffer_size_bytes, self.arraysize)
        return self

    def schemas(self, catalog_name: Optional[str] = None,
                schema_name: Optional[str] = None) -> "Cursor":
        """
        Get schemas corresponding to the catalog_name and schema_name.

        Names can contain % wildcards.
        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()
        execute_response = self.thrift_backend.get_schemas(
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
            catalog_name=catalog_name,
            schema_name=schema_name)
        self.active_result_set = ResultSet(self.connection, execute_response, self.thrift_backend,
                                           self.buffer_size_bytes, self.arraysize)
        return self

    def tables(self,
               catalog_name: Optional[str] = None,
               schema_name: Optional[str] = None,
               table_name: Optional[str] = None,
               table_types: List[str] = None) -> "Cursor":
        """
        Get tables corresponding to the catalog_name, schema_name and table_name.

        Names can contain % wildcards.
        :returns self
        """
        if table_types is None:
            table_types = []
        self._check_not_closed()
        self._close_and_clear_active_result_set()

        execute_response = self.thrift_backend.get_tables(
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            table_types=table_types)
        self.active_result_set = ResultSet(self.connection, execute_response, self.thrift_backend,
                                           self.buffer_size_bytes, self.arraysize)
        return self

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

        execute_response = self.thrift_backend.get_columns(
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name)
        self.active_result_set = ResultSet(self.connection, execute_response, self.thrift_backend,
                                           self.buffer_size_bytes, self.arraysize)
        return self

    def fetchall(self) -> List[Tuple]:
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences.

        A databricks.sql.Error (or subclass) exception is raised if the previous call to
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

        An databricks.sql.Error (or subclass) exception is raised if the previous call to
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

        A databricks.sql.Error (or subclass) exception is raised if the previous call
        to execute did not produce any result set or no call was issued yet.
        """
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchmany(n_rows)
        else:
            raise Error("There is no active result set")

    def fetchall_arrow(self):
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchall_arrow()
        else:
            raise Error("There is no active result set")

    def fetchmany_arrow(self, n_rows):
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchmany_arrow(n_rows)
        else:
            raise Error("There is no active result set")

    def cancel(self) -> None:
        """
        Cancel a running command.

        The command should be closed to free resources from the server.
        This method can be called from another thread.
        """
        if self.active_op_handle is not None:
            self.thrift_backend.cancel_command(self.active_op_handle)
        else:
            logger.warning("Attempting to cancel a command, but there is no "
                           "currently executing command")

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
                 execute_response: ExecuteResponse,
                 thrift_backend: ThriftBackend,
                 result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
                 arraysize: int = 10000):
        """
        A ResultSet manages the results of a single command.

        :param connection: The parent connection that was used to execute this command
        :param execute_response: A `ExecuteResponse` class returned by a command execution
        :param result_buffer_size_bytes: The size (in bytes) of the internal buffer + max fetch
        amount :param arraysize: The max number of rows to fetch at a time (PEP-249)
        """
        self.connection = connection
        self.command_id = execute_response.command_handle
        self.op_state = execute_response.status
        self.has_been_closed_server_side = execute_response.has_been_closed_server_side
        self.has_more_rows = execute_response.has_more_rows
        self.buffer_size_bytes = result_buffer_size_bytes
        self.arraysize = arraysize
        self.thrift_backend = thrift_backend
        self.description = execute_response.description
        self._arrow_schema = execute_response.arrow_schema
        self._next_row_index = 0

        if execute_response.arrow_queue:
            # In this case the server has taken the fast path and returned an initial batch of
            # results
            self.results = execute_response.arrow_queue
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

    def _fill_results_buffer(self):
        results, has_more_rows = self.thrift_backend.fetch_results(
            op_handle=self.command_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            expected_row_start_offset=self._next_row_index,
            arrow_schema=self._arrow_schema,
            description=self.description)
        self.results = results
        self.has_more_rows = has_more_rows

    def _convert_arrow_table(self, table):
        n_rows, _ = table.shape
        list_repr = [[col[row_index].as_py() for col in table.itercolumns()]
                     for row_index in range(n_rows)]
        return list_repr

    def fetchmany_arrow(self, n_rows: int) -> pyarrow.Table:
        """
        Fetch the next set of rows of a query result, returning a PyArrow table.

        An empty sequence is returned when no more rows are available.
        """
        if n_rows < 0:
            raise ValueError("n_rows argument for fetchmany is %s but must be >= 0", n_rows)
        results = self.results.next_n_rows(n_rows)
        n_remaining_rows = n_rows - results.num_rows
        self._next_row_index += results.num_rows

        while n_remaining_rows > 0 and not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            results = pyarrow.concat_tables([results, partial_results])
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        return results

    def fetchall_arrow(self) -> pyarrow.Table:
        """Fetch all (remaining) rows of a query result, returning them as a PyArrow table."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            results = pyarrow.concat_tables([results, partial_results])
            self._next_row_index += partial_results.num_rows

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
            if self.op_state != self.thrift_backend.CLOSED_OP_STATE and not self.has_been_closed_server_side \
              and self.connection.open:
                self.thrift_backend.close_command(self.command_id)
        finally:
            self.has_been_closed_server_side = True
            self.op_state = self.thrift_backend.CLOSED_OP_STATE

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
