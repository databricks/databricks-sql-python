import decimal
import json
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING

import pandas
import pyarrow
import requests

from databricks.sql import *
from databricks.sql import __version__
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.exc import (
    CursorAlreadyClosedError,
    OperationalError,
    SessionAlreadyClosedError,
)
from databricks.sql.experimental.oauth_persistence import OAuthPersistence
from databricks.sql.parameters.choose import prepare_parameters_and_statement
from databricks.sql.parameters.native import TParameterCollection
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.thrift_backend import ThriftBackend
from databricks.sql.types import Row
from databricks.sql.utils import ExecuteResponse

from databricks.sql.results import ResultSet


from databricks.sql.ae import AsyncExecution, AsyncExecutionStatus
from uuid import UUID

logger = logging.getLogger(__name__)

DEFAULT_RESULT_BUFFER_SIZE_BYTES = 104857600
DEFAULT_ARRAY_SIZE = 100000


class Connection:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: Optional[str] = None,
        http_headers: Optional[List[Tuple[str, str]]] = None,
        session_configuration: Dict[str, Any] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        _use_arrow_native_complex_types: Optional[bool] = True,
        **kwargs,
    ) -> None:
        """
        Connect to a Databricks SQL endpoint or a Databricks cluster.

        Parameters:
            :param server_hostname: Databricks instance host name.
            :param http_path: Http path either to a DBSQL endpoint (e.g. /sql/1.0/endpoints/1234567890abcdef)
                or to a DBR interactive cluster (e.g. /sql/protocolv1/o/1234567890123456/1234-123456-slid123)
            :param access_token: `str`, optional
                Http Bearer access token, e.g. Databricks Personal Access Token.
                Unless if you use auth_type=`databricks-oauth` you need to pass `access_token.
                Examples:
                        ```
                         connection = sql.connect(
                            server_hostname='dbc-12345.staging.cloud.databricks.com',
                            http_path='sql/protocolv1/o/6789/12abc567',
                            access_token='dabpi12345678'
                         )
                        ```
            :param http_headers: An optional list of (k, v) pairs that will be set as Http headers on every request
            :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
                Execute the SQL command `SET -v` to get a full list of available commands.
            :param catalog: An optional initial catalog to use. Requires DBR version 9.0+
            :param schema: An optional initial schema to use. Requires DBR version 9.0+

        Other Parameters:
            use_inline_params: `boolean` | str, optional (default is False)
                When True, parameterized calls to cursor.execute() will try to render parameter values inline with the
                query text instead of using native bound parameters supported in DBR 14.1 and above. This connector will attempt to
                sanitise parameterized inputs to prevent SQL injection.  The inline parameter approach is maintained for
                legacy purposes and will be deprecated in a future release. When this parameter is `True` you will see
                a warning log message. To suppress this log message, set `use_inline_params="silent"`.
            auth_type: `str`, optional
                `databricks-oauth` : to use oauth with fine-grained permission scopes, set to `databricks-oauth`.
                This is currently in private preview for Databricks accounts on AWS.
                This supports User to Machine OAuth authentication for Databricks on AWS with
                any IDP configured. This is only for interactive python applications and open a browser window.
                Note this is beta (private preview)

            oauth_client_id: `str`, optional
                custom oauth client_id. If not specified, it will use the built-in client_id of databricks-sql-python.

            oauth_redirect_port: `int`, optional
                port of the oauth redirect uri (localhost). This is required when custom oauth client_id
                `oauth_client_id` is set

            experimental_oauth_persistence: configures preferred storage for persisting oauth tokens.
                This has to be a class implementing `OAuthPersistence`.
                When `auth_type` is set to `databricks-oauth` without persisting the oauth token in a persistence storage
                the oauth tokens will only be maintained in memory and if the python process restarts the end user
                will have to login again.
                Note this is beta (private preview)

                For persisting the oauth token in a prod environment you should subclass and implement OAuthPersistence

                from databricks.sql.experimental.oauth_persistence import OAuthPersistence, OAuthToken
                class MyCustomImplementation(OAuthPersistence):
                    def __init__(self, file_path):
                        self._file_path = file_path

                    def persist(self, token: OAuthToken):
                        # implement this method to persist token.refresh_token and token.access_token

                    def read(self) -> Optional[OAuthToken]:
                        # implement this method to return an instance of the persisted token


                    connection = sql.connect(
                        server_hostname='dbc-12345.staging.cloud.databricks.com',
                        http_path='sql/protocolv1/o/6789/12abc567',
                        auth_type="databricks-oauth",
                        experimental_oauth_persistence=MyCustomImplementation()
                    )

                For development purpose you can use the existing `DevOnlyFilePersistence` which stores the
                raw oauth token in the provided file path. Please note this is only for development and for prod you should provide your
                own implementation of OAuthPersistence.

                Examples:
                ```
                        # for development only
                        from databricks.sql.experimental.oauth_persistence import DevOnlyFilePersistence

                        connection = sql.connect(
                            server_hostname='dbc-12345.staging.cloud.databricks.com',
                            http_path='sql/protocolv1/o/6789/12abc567',
                            auth_type="databricks-oauth",
                            experimental_oauth_persistence=DevOnlyFilePersistence("~/dev-oauth.json")
                        )
                ```
            :param _use_arrow_native_complex_types: `bool`, optional
                Controls whether a complex type field value is returned as a string or as a native Arrow type. Defaults to True.
                When True:
                    MAP is returned as List[Tuple[str, Any]]
                    STRUCT is returned as Dict[str, Any]
                    ARRAY is returned as numpy.ndarray
                When False, complex types are returned as a strings. These are generally deserializable as JSON.
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
        # _disable_pandas
        #  In case the deserialisation through pandas causes any issues, it can be disabled with
        #  this flag.
        # _use_arrow_native_decimals
        # Databricks runtime will return native Arrow types for decimals instead of Arrow strings
        # (True by default)
        # _use_arrow_native_timestamps
        # Databricks runtime will return native Arrow types for timestamps instead of Arrow strings
        # (True by default)
        # use_cloud_fetch
        # Enable use of cloud fetch to extract large query results in parallel via cloud storage

        if access_token:
            access_token_kv = {"access_token": access_token}
            kwargs = {**kwargs, **access_token_kv}

        self.open = False
        self.host = server_hostname
        self.port = kwargs.get("_port", 443)
        self.disable_pandas = kwargs.get("_disable_pandas", False)
        self.lz4_compression = kwargs.get("enable_query_result_lz4_compression", True)

        auth_provider = get_python_sql_connector_auth_provider(
            server_hostname, **kwargs
        )

        if not kwargs.get("_user_agent_entry"):
            useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)
        else:
            useragent_header = "{}/{} ({})".format(
                USER_AGENT_NAME, __version__, kwargs.get("_user_agent_entry")
            )

        base_headers = [("User-Agent", useragent_header)]

        self.thrift_backend = ThriftBackend(
            self.host,
            self.port,
            http_path,
            (http_headers or []) + base_headers,
            auth_provider,
            _use_arrow_native_complex_types=_use_arrow_native_complex_types,
            **kwargs,
        )

        self._open_session_resp = self.thrift_backend.open_session(
            session_configuration, catalog, schema
        )
        self._session_handle = self._open_session_resp.sessionHandle
        self.protocol_version = self.get_protocol_version(self._open_session_resp)
        self.use_cloud_fetch = kwargs.get("use_cloud_fetch", True)
        self.open = True
        logger.info("Successfully opened session " + str(self.get_session_id_hex()))
        self._cursors = []  # type: List[Cursor]

        self.use_inline_params = self._set_use_inline_params_with_warning(
            kwargs.get("use_inline_params", False)
        )

    def _set_use_inline_params_with_warning(self, value: Union[bool, str]):
        """Valid values are True, False, and "silent"

        False: Use native parameters
        True: Use inline parameters and log a warning
        "silent": Use inline parameters and don't log a warning
        """

        if value is False:
            return False

        if value not in [True, "silent"]:
            raise ValueError(
                f"Invalid value for use_inline_params: {value}. "
                + 'Valid values are True, False, and "silent"'
            )

        if value is True:
            logger.warning(
                "Parameterised queries executed with this client will use the inline parameter approach."
                "This approach will be deprecated in a future release. Consider using native parameters."
                "Learn more: https://github.com/databricks/databricks-sql-python/tree/main/docs/parameters.md"
                'To suppress this warning, set use_inline_params="silent"'
            )

        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        if self.open:
            logger.debug(
                "Closing unclosed connection for session "
                "{}".format(self.get_session_id_hex())
            )
            try:
                self._close(close_cursors=False)
            except OperationalError as e:
                # Close on best-effort basis.
                logger.debug("Couldn't close unclosed connection: {}".format(e.message))

    def get_session_id(self):
        return self.thrift_backend.handle_to_id(self._session_handle)

    @staticmethod
    def get_protocol_version(openSessionResp):
        """
        Since the sessionHandle will sometimes have a serverProtocolVersion, it takes
        precedence over the serverProtocolVersion defined in the OpenSessionResponse.
        """
        if (
            openSessionResp.sessionHandle
            and hasattr(openSessionResp.sessionHandle, "serverProtocolVersion")
            and openSessionResp.sessionHandle.serverProtocolVersion
        ):
            return openSessionResp.sessionHandle.serverProtocolVersion
        return openSessionResp.serverProtocolVersion

    @staticmethod
    def server_parameterized_queries_enabled(protocolVersion):
        if (
            protocolVersion
            and protocolVersion >= ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8
        ):
            return True
        else:
            return False

    def get_session_id_hex(self):
        return self.thrift_backend.handle_to_hex_id(self._session_handle)

    def cursor(
        self,
        arraysize: int = DEFAULT_ARRAY_SIZE,
        buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
    ) -> "Cursor":
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
            result_buffer_size_bytes=buffer_size_bytes,
        )
        self._cursors.append(cursor)
        return cursor

    def close(self) -> None:
        """Close the underlying session and mark all associated cursors as closed."""
        self._close()

    def _close(self, close_cursors=True) -> None:
        if close_cursors:
            for cursor in self._cursors:
                cursor.close()

        logger.info(f"Closing session {self.get_session_id_hex()}")
        if not self.open:
            logger.debug("Session appears to have been closed already")

        try:
            self.thrift_backend.close_session(self._session_handle)
        except RequestError as e:
            if isinstance(e.args[1], SessionAlreadyClosedError):
                logger.info("Session was closed by a prior request")
        except DatabaseError as e:
            if "Invalid SessionHandle" in str(e):
                logger.warning(
                    f"Attempted to close session that was already closed: {e}"
                )
            else:
                logger.warning(
                    f"Attempt to close session raised an exception at the server: {e}"
                )
        except Exception as e:
            logger.error(f"Attempt to close session raised a local exception: {e}")

        self.open = False

    def commit(self):
        """No-op because Databricks does not support transactions"""
        pass

    def rollback(self):
        raise NotSupportedError("Transactions are not supported on Databricks")

    def execute_async(
        self, query: str, parameters: Optional[TParameterCollection] = None
    ) -> "AsyncExecution":
        """Begin executing without waiting for it to complete.

        Args:
            query: The query text to be executed
            parameters: Parameters that will be bound to the variable markers contained in the query text. The format
            of these variable markers depends on whether the connection was instantiated with use_inline_params=True. See
            :class:`client.Connection` for more details.

        Returns:
            An AsyncExecution object that can be used to poll for status and retrieve results.
        """

        # this code is copied from `execute`
        prepared_operation, prepared_params = prepare_parameters_and_statement(
            query, parameters, self
        )

        with self.cursor() as cursor:
            execute_statement_resp = self.thrift_backend.async_execute_statement(
                statement=prepared_operation,
                session_handle=self._session_handle,
                max_rows=cursor.arraysize,
                max_bytes=cursor.buffer_size_bytes,
                lz4_compression=self.lz4_compression,
                cursor=cursor,
                use_cloud_fetch=self.use_cloud_fetch,
                parameters=prepared_params,
            )

        return AsyncExecution.from_thrift_response(
            connection=self,
            thrift_backend=self.thrift_backend,
            resp=execute_statement_resp,
        )

    def get_async_execution(
        self, query_id: Union[str, UUID], query_secret: Union[str, UUID]
    ) -> "AsyncExecution":
        """Get an AsyncExecution object for an existing query.

        Args:
            query_id: The query id of the query to retrieve
            query_secret: The query secret of the query to retrieve

        Returns:
            An AsyncExecution object that can be used to poll for status and retrieve results.
        """

        if isinstance(query_id, UUID):
            _qid = query_id
        else:
            _qid = UUID(hex=query_id)

        if isinstance(query_secret, UUID):
            _qs = query_secret
        else:
            _qs = UUID(hex=query_secret)

        return AsyncExecution.from_query_id_and_secret(
            connection=self,
            thrift_backend=self.thrift_backend,
            query_id=_qid,
            query_secret=_qs,
        )


class Cursor:
    def __init__(
        self,
        connection: Connection,
        thrift_backend: ThriftBackend,
        result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
        arraysize: int = DEFAULT_ARRAY_SIZE,
    ) -> None:
        """
        These objects represent a database cursor, which is used to manage the context of a fetch
        operation.

        Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
        visible by other cursors or connections.
        """
        self.connection = connection
        self.rowcount = -1  # Return -1 as this is not supported
        self.buffer_size_bytes = result_buffer_size_bytes
        self.active_result_set: Union[ResultSet, None] = None
        self.arraysize = arraysize
        # Note that Cursor closed => active result set closed, but not vice versa
        self.open = True
        self.executing_command_id = None
        self.thrift_backend = thrift_backend
        self.active_op_handle = None
        self.lastrowid = None

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

    def _handle_staging_operation(
        self, staging_allowed_local_path: Union[None, str, List[str]]
    ):
        """Fetch the HTTP request instruction from a staging ingestion command
        and call the designated handler.

        Raise an exception if localFile is specified by the server but the localFile
        is not descended from staging_allowed_local_path.
        """

        if isinstance(staging_allowed_local_path, type(str())):
            _staging_allowed_local_paths = [staging_allowed_local_path]
        elif isinstance(staging_allowed_local_path, type(list())):
            _staging_allowed_local_paths = staging_allowed_local_path
        else:
            raise Error(
                "You must provide at least one staging_allowed_local_path when initialising a connection to perform ingestion commands"
            )

        abs_staging_allowed_local_paths = [
            os.path.abspath(i) for i in _staging_allowed_local_paths
        ]

        assert self.active_result_set is not None
        row = self.active_result_set.fetchone()
        assert row is not None

        # Must set to None in cases where server response does not include localFile
        abs_localFile = None

        # Default to not allow staging operations
        allow_operation = False
        if getattr(row, "localFile", None):
            abs_localFile = os.path.abspath(row.localFile)
            for abs_staging_allowed_local_path in abs_staging_allowed_local_paths:
                # If the indicated local file matches at least one allowed base path, allow the operation
                if (
                    os.path.commonpath([abs_localFile, abs_staging_allowed_local_path])
                    == abs_staging_allowed_local_path
                ):
                    allow_operation = True
                else:
                    continue
            if not allow_operation:
                raise Error(
                    "Local file operations are restricted to paths within the configured staging_allowed_local_path"
                )

        # TODO: Experiment with DBR sending real headers.
        # The specification says headers will be in JSON format but the current null value is actually an empty list []
        handler_args = {
            "presigned_url": row.presignedUrl,
            "local_file": abs_localFile,
            "headers": json.loads(row.headers or "{}"),
        }

        logger.debug(
            f"Attempting staging operation indicated by server: {row.operation} - {getattr(row, 'localFile', '')}"
        )

        # TODO: Create a retry loop here to re-attempt if the request times out or fails
        if row.operation == "GET":
            return self._handle_staging_get(**handler_args)
        elif row.operation == "PUT":
            return self._handle_staging_put(**handler_args)
        elif row.operation == "REMOVE":
            # Local file isn't needed to remove a remote resource
            handler_args.pop("local_file")
            return self._handle_staging_remove(**handler_args)
        else:
            raise Error(
                f"Operation {row.operation} is not supported. "
                + "Supported operations are GET, PUT, and REMOVE"
            )

    def _handle_staging_put(
        self, presigned_url: str, local_file: str, headers: dict = None
    ):
        """Make an HTTP PUT request

        Raise an exception if request fails. Returns no data.
        """

        if local_file is None:
            raise Error("Cannot perform PUT without specifying a local_file")

        with open(local_file, "rb") as fh:
            r = requests.put(url=presigned_url, data=fh, headers=headers)

        # fmt: off
        # Design borrowed from: https://stackoverflow.com/a/2342589/5093960
            
        OK = requests.codes.ok                  # 200
        CREATED = requests.codes.created        # 201
        ACCEPTED = requests.codes.accepted      # 202
        NO_CONTENT = requests.codes.no_content  # 204

        # fmt: on

        if r.status_code not in [OK, CREATED, NO_CONTENT, ACCEPTED]:
            raise Error(
                f"Staging operation over HTTP was unsuccessful: {r.status_code}-{r.text}"
            )

        if r.status_code == ACCEPTED:
            logger.debug(
                f"Response code {ACCEPTED} from server indicates ingestion command was accepted "
                + "but not yet applied on the server. It's possible this command may fail later."
            )

    def _handle_staging_get(
        self, local_file: str, presigned_url: str, headers: dict = None
    ):
        """Make an HTTP GET request, create a local file with the received data

        Raise an exception if request fails. Returns no data.
        """

        if local_file is None:
            raise Error("Cannot perform GET without specifying a local_file")

        r = requests.get(url=presigned_url, headers=headers)

        # response.ok verifies the status code is not between 400-600.
        # Any 2xx or 3xx will evaluate r.ok == True
        if not r.ok:
            raise Error(
                f"Staging operation over HTTP was unsuccessful: {r.status_code}-{r.text}"
            )

        with open(local_file, "wb") as fp:
            fp.write(r.content)

    def _handle_staging_remove(self, presigned_url: str, headers: dict = None):
        """Make an HTTP DELETE request to the presigned_url"""

        r = requests.delete(url=presigned_url, headers=headers)

        if not r.ok:
            raise Error(
                f"Staging operation over HTTP was unsuccessful: {r.status_code}-{r.text}"
            )

    def execute(
        self,
        operation: str,
        parameters: Optional[TParameterCollection] = None,
    ) -> "Cursor":
        """
        Execute a query and wait for execution to complete.

        The parameterisation behaviour of this method depends on which parameter approach is used:
            - With INLINE mode, parameters are rendered inline with the query text
            - With NATIVE mode (default), parameters are sent to the server separately for binding

        This behaviour is controlled by the `use_inline_params` argument passed when building a connection.

        The paramstyle for these approaches is different:

        If the connection was instantiated with use_inline_params=False (default), then parameters
        should be given in PEP-249 `named` paramstyle like :param_name. Parameters passed by positionally
        are indicated using a `?` in the query text.

        If the connection was instantiated with use_inline_params=True, then parameters
        should be given in PEP-249 `pyformat` paramstyle like %(param_name)s. Parameters passed by positionally
        are indicated using a `%s` marker in the query. Note: this approach is not recommended as it can break
        your SQL query syntax and will be removed in a future release.

        ```python
        inline_operation = "SELECT * FROM table WHERE field = %(some_value)s"
        native_operation = "SELECT * FROM table WHERE field = :some_value"
        parameters = {"some_value": "foo"}
        ```

        Both will result in the query equivalent to "SELECT * FROM table WHERE field = 'foo'
        being sent to the server

        :returns self
        """

        prepared_operation, prepared_params = prepare_parameters_and_statement(
            operation, parameters, self.connection
        )

        self._check_not_closed()
        self._close_and_clear_active_result_set()
        execute_response = self.thrift_backend.execute_command(
            operation=prepared_operation,
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            lz4_compression=self.connection.lz4_compression,
            cursor=self,
            use_cloud_fetch=self.connection.use_cloud_fetch,
            parameters=prepared_params,
        )
        self.active_result_set = ResultSet(
            self.connection,
            execute_response,
            self.thrift_backend,
            self.buffer_size_bytes,
            self.arraysize,
        )

        if execute_response.is_staging_operation:
            self._handle_staging_operation(
                staging_allowed_local_path=self.thrift_backend.staging_allowed_local_path
            )

        return self

    def executemany(self, operation, seq_of_parameters):
        """
        Execute the operation once for every set of passed in parameters.

        This will issue N sequential request to the database where N is the length of the provided sequence.
        No optimizations of the query (like batching) will be performed.

        Only the final result set is retained.

        :returns self
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
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
            cursor=self,
        )
        self.active_result_set = ResultSet(
            self.connection,
            execute_response,
            self.thrift_backend,
            self.buffer_size_bytes,
            self.arraysize,
        )
        return self

    def schemas(
        self, catalog_name: Optional[str] = None, schema_name: Optional[str] = None
    ) -> "Cursor":
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
            schema_name=schema_name,
        )
        self.active_result_set = ResultSet(
            self.connection,
            execute_response,
            self.thrift_backend,
            self.buffer_size_bytes,
            self.arraysize,
        )
        return self

    def tables(
        self,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        table_types: List[str] = None,
    ) -> "Cursor":
        """
        Get tables corresponding to the catalog_name, schema_name and table_name.

        Names can contain % wildcards.
        :returns self
        """
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
            table_types=table_types,
        )
        self.active_result_set = ResultSet(
            self.connection,
            execute_response,
            self.thrift_backend,
            self.buffer_size_bytes,
            self.arraysize,
        )
        return self

    def columns(
        self,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> "Cursor":
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
            column_name=column_name,
        )
        self.active_result_set = ResultSet(
            self.connection,
            execute_response,
            self.thrift_backend,
            self.buffer_size_bytes,
            self.arraysize,
        )
        return self

    def fetchall(self) -> List[Row]:
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

    def fetchone(self) -> Optional[Row]:
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

    def fetchmany(self, size: int) -> List[Row]:
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
            return self.active_result_set.fetchmany(size)
        else:
            raise Error("There is no active result set")

    def fetchall_arrow(self) -> pyarrow.Table:
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchall_arrow()
        else:
            raise Error("There is no active result set")

    def fetchmany_arrow(self, size) -> pyarrow.Table:
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchmany_arrow(size)
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
            logger.warning(
                "Attempting to cancel a command, but there is no "
                "currently executing command"
            )

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

    @property
    def rownumber(self):
        """This read-only attribute should provide the current 0-based index of the cursor in the
        result set.

        The index can be seen as index of the cursor in a sequence (the result set). The next fetch
        operation will fetch the row indexed by ``rownumber`` in that sequence.
        """
        return self.active_result_set.rownumber if self.active_result_set else 0

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass
