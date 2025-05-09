import time
from typing import Dict, Tuple, List, Optional, Any, Union, Sequence

import pandas

try:
    import pyarrow
except ImportError:
    pyarrow = None
import requests
import json
import os
import decimal
from uuid import UUID

from databricks.sql import __version__
from databricks.sql import *
from databricks.sql.exc import (
    OperationalError,
    SessionAlreadyClosedError,
    CursorAlreadyClosedError,
)
from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.thrift_backend import ThriftBackend
from databricks.sql.utils import (
    ExecuteResponse,
    ParamEscaper,
    inject_parameters,
    transform_paramstyle,
    ColumnTable,
    ColumnQueue,
)
from databricks.sql.parameters.native import (
    DbsqlParameterBase,
    TDbsqlParameter,
    TParameterDict,
    TParameterSequence,
    TParameterCollection,
    ParameterStructure,
    dbsql_parameter_from_primitive,
    ParameterApproach,
)


from databricks.sql.types import Row, SSLOptions
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.experimental.oauth_persistence import OAuthPersistence

from databricks.sql.thrift_api.TCLIService.ttypes import (
    TSparkParameter,
    TOperationState,
)


logger = logging.getLogger(__name__)

if pyarrow is None:
    logger.warning(
        "[WARN] pyarrow is not installed by default since databricks-sql-connector 4.0.0,"
        "any arrow specific api (e.g. fetchmany_arrow) and cloud fetch will be disabled."
        "If you need these features, please run pip install pyarrow or pip install databricks-sql-connector[pyarrow] to install"
    )

DEFAULT_RESULT_BUFFER_SIZE_BYTES = 104857600
DEFAULT_ARRAY_SIZE = 100000

NO_NATIVE_PARAMS: List = []


class Connection:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: Optional[str] = None,
        http_headers: Optional[List[Tuple[str, str]]] = None,
        session_configuration: Optional[Dict[str, Any]] = None,
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
            auth_type: `str`, optional (default is databricks-oauth if neither `access_token` nor `tls_client_cert_file` is set)
                `databricks-oauth` : to use Databricks OAuth with fine-grained permission scopes, set to `databricks-oauth`.
                `azure-oauth` : to use Microsoft Entra ID OAuth flow, set to `azure-oauth`.

            oauth_client_id: `str`, optional
                custom oauth client_id. If not specified, it will use the built-in client_id of databricks-sql-python.

            oauth_redirect_port: `int`, optional
                port of the oauth redirect uri (localhost). This is required when custom oauth client_id
                `oauth_client_id` is set

            user_agent_entry: `str`, optional
                A custom tag to append to the User-Agent header. This is typically used by partners to identify their applications.. If not specified, it will use the default user agent PyDatabricksSqlConnector

            experimental_oauth_persistence: configures preferred storage for persisting oauth tokens.
                This has to be a class implementing `OAuthPersistence`.
                When `auth_type` is set to `databricks-oauth` or `azure-oauth` without persisting the oauth token in a
                persistence storage the oauth tokens will only be maintained in memory and if the python process
                restarts the end user will have to login again.
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
        # _use_cert_as_auth
        #  Use a TLS cert instead of a token
        # _enable_ssl
        #  Connect over HTTP instead of HTTPS
        # _port
        #  Which port to connect to
        # _skip_routing_headers:
        #  Don't set routing headers if set to True (for use when connecting directly to server)
        # _tls_no_verify
        #   Set to True (Boolean) to completely disable SSL verification.
        # _tls_verify_hostname
        #   Set to False (Boolean) to disable SSL hostname verification, but check certificate.
        # _tls_trusted_ca_file
        #   Set to the path of the file containing trusted CA certificates for server certificate
        #   verification. If not provide, uses system truststore.
        # _tls_client_cert_file, _tls_client_cert_key_file, _tls_client_cert_key_password
        #   Set client SSL certificate.
        #   See https://docs.python.org/3/library/ssl.html#ssl.SSLContext.load_cert_chain
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

        user_agent_entry = kwargs.get("user_agent_entry")
        if user_agent_entry is None:
            user_agent_entry = kwargs.get("_user_agent_entry")
            if user_agent_entry is not None:
                logger.warning(
                    "[WARN] Parameter '_user_agent_entry' is deprecated; use 'user_agent_entry' instead. "
                    "This parameter will be removed in the upcoming releases."
                )

        if user_agent_entry:
            useragent_header = "{}/{} ({})".format(
                USER_AGENT_NAME, __version__, user_agent_entry
            )
        else:
            useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)

        base_headers = [("User-Agent", useragent_header)]

        self._ssl_options = SSLOptions(
            # Double negation is generally a bad thing, but we have to keep backward compatibility
            tls_verify=not kwargs.get(
                "_tls_no_verify", False
            ),  # by default - verify cert and host
            tls_verify_hostname=kwargs.get("_tls_verify_hostname", True),
            tls_trusted_ca_file=kwargs.get("_tls_trusted_ca_file"),
            tls_client_cert_file=kwargs.get("_tls_client_cert_file"),
            tls_client_cert_key_file=kwargs.get("_tls_client_cert_key_file"),
            tls_client_cert_key_password=kwargs.get("_tls_client_cert_key_password"),
        )

        self.thrift_backend = ThriftBackend(
            self.host,
            self.port,
            http_path,
            (http_headers or []) + base_headers,
            auth_provider,
            ssl_options=self._ssl_options,
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

    # The ideal return type for this method is perhaps Self, but that was not added until 3.11, and we support pre-3.11 pythons, currently.
    def __enter__(self) -> "Connection":
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
        self.escaper = ParamEscaper()
        self.lastrowid = None

        self.ASYNC_DEFAULT_POLLING_INTERVAL = 2

    # The ideal return type for this method is perhaps Self, but that was not added until 3.11, and we support pre-3.11 pythons, currently.
    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self):
        if self.active_result_set:
            for row in self.active_result_set:
                yield row
        else:
            raise Error("There is no active result set")

    def _determine_parameter_approach(
        self, params: Optional[TParameterCollection]
    ) -> ParameterApproach:
        """Encapsulates the logic for choosing whether to send parameters in native vs inline mode

        If params is None then ParameterApproach.NONE is returned.
        If self.use_inline_params is True then inline mode is used.
        If self.use_inline_params is False, then check if the server supports them and proceed.
            Else raise an exception.

        Returns a ParameterApproach enumeration or raises an exception

        If inline approach is used when the server supports native approach, a warning is logged
        """

        if params is None:
            return ParameterApproach.NONE

        if self.connection.use_inline_params:
            return ParameterApproach.INLINE

        else:
            return ParameterApproach.NATIVE

    def _all_dbsql_parameters_are_named(self, params: List[TDbsqlParameter]) -> bool:
        """Return True if all members of the list have a non-null .name attribute"""
        return all([i.name is not None for i in params])

    def _normalize_tparametersequence(
        self, params: TParameterSequence
    ) -> List[TDbsqlParameter]:
        """Retains the same order as the input list."""

        output: List[TDbsqlParameter] = []
        for p in params:
            if isinstance(p, DbsqlParameterBase):
                output.append(p)
            else:
                output.append(dbsql_parameter_from_primitive(value=p))

        return output

    def _normalize_tparameterdict(
        self, params: TParameterDict
    ) -> List[TDbsqlParameter]:
        return [
            dbsql_parameter_from_primitive(value=value, name=name)
            for name, value in params.items()
        ]

    def _normalize_tparametercollection(
        self, params: Optional[TParameterCollection]
    ) -> List[TDbsqlParameter]:
        if params is None:
            return []
        if isinstance(params, dict):
            return self._normalize_tparameterdict(params)
        if isinstance(params, Sequence):
            return self._normalize_tparametersequence(list(params))

    def _determine_parameter_structure(
        self,
        parameters: List[TDbsqlParameter],
    ) -> ParameterStructure:
        all_named = self._all_dbsql_parameters_are_named(parameters)
        if all_named:
            return ParameterStructure.NAMED
        else:
            return ParameterStructure.POSITIONAL

    def _prepare_inline_parameters(
        self, stmt: str, params: Optional[Union[Sequence, Dict[str, Any]]]
    ) -> Tuple[str, List]:
        """Return a statement and list of native parameters to be passed to thrift_backend for execution

        :stmt:
            A string SQL query containing parameter markers of PEP-249 paramstyle `pyformat`.
            For example `%(param)s`.

        :params:
            An iterable of parameter values to be rendered inline. If passed as a Dict, the keys
            must match the names of the markers included in :stmt:. If passed as a List, its length
            must equal the count of parameter markers in :stmt:.

        Returns a tuple of:
            stmt: the passed statement with the param markers replaced by literal rendered values
            params: an empty list representing the native parameters to be passed with this query.
                The list is always empty because native parameters are never used under the inline approach
        """

        escaped_values = self.escaper.escape_args(params)
        rendered_statement = inject_parameters(stmt, escaped_values)

        return rendered_statement, NO_NATIVE_PARAMS

    def _prepare_native_parameters(
        self,
        stmt: str,
        params: List[TDbsqlParameter],
        param_structure: ParameterStructure,
    ) -> Tuple[str, List[TSparkParameter]]:
        """Return a statement and a list of native parameters to be passed to thrift_backend for execution

        :stmt:
            A string SQL query containing parameter markers of PEP-249 paramstyle `named`.
            For example `:param`.

        :params:
            An iterable of parameter values to be sent natively. If passed as a Dict, the keys
            must match the names of the markers included in :stmt:. If passed as a List, its length
            must equal the count of parameter markers in :stmt:. In list form, any member of the list
            can be wrapped in a DbsqlParameter class.

        Returns a tuple of:
            stmt: the passed statement` with the param markers replaced by literal rendered values
            params: a list of TSparkParameters that will be passed in native mode
        """

        stmt = stmt
        output = [
            p.as_tspark_param(named=param_structure == ParameterStructure.NAMED)
            for p in params
        ]

        return stmt, output

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

        # May be real headers, or could be json string
        headers = (
            json.loads(row.headers) if isinstance(row.headers, str) else row.headers
        )

        handler_args = {
            "presigned_url": row.presignedUrl,
            "local_file": abs_localFile,
            "headers": dict(headers) or {},
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
        self, presigned_url: str, local_file: str, headers: Optional[dict] = None
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
        self, local_file: str, presigned_url: str, headers: Optional[dict] = None
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

    def _handle_staging_remove(
        self, presigned_url: str, headers: Optional[dict] = None
    ):
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
        enforce_embedded_schema_correctness=False,
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

        param_approach = self._determine_parameter_approach(parameters)
        if param_approach == ParameterApproach.NONE:
            prepared_params = NO_NATIVE_PARAMS
            prepared_operation = operation

        elif param_approach == ParameterApproach.INLINE:
            prepared_operation, prepared_params = self._prepare_inline_parameters(
                operation, parameters
            )
        elif param_approach == ParameterApproach.NATIVE:
            normalized_parameters = self._normalize_tparametercollection(parameters)
            param_structure = self._determine_parameter_structure(normalized_parameters)
            transformed_operation = transform_paramstyle(
                operation, normalized_parameters, param_structure
            )
            prepared_operation, prepared_params = self._prepare_native_parameters(
                transformed_operation, normalized_parameters, param_structure
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
            async_op=False,
            enforce_embedded_schema_correctness=enforce_embedded_schema_correctness,
        )
        self.active_result_set = ResultSet(
            self.connection,
            execute_response,
            self.thrift_backend,
            self.buffer_size_bytes,
            self.arraysize,
            self.connection.use_cloud_fetch,
        )

        if execute_response.is_staging_operation:
            self._handle_staging_operation(
                staging_allowed_local_path=self.thrift_backend.staging_allowed_local_path
            )

        return self

    def execute_async(
        self,
        operation: str,
        parameters: Optional[TParameterCollection] = None,
        enforce_embedded_schema_correctness=False,
    ) -> "Cursor":
        """

        Execute a query and do not wait for it to complete and just move ahead

        :param operation:
        :param parameters:
        :return:
        """
        param_approach = self._determine_parameter_approach(parameters)
        if param_approach == ParameterApproach.NONE:
            prepared_params = NO_NATIVE_PARAMS
            prepared_operation = operation

        elif param_approach == ParameterApproach.INLINE:
            prepared_operation, prepared_params = self._prepare_inline_parameters(
                operation, parameters
            )
        elif param_approach == ParameterApproach.NATIVE:
            normalized_parameters = self._normalize_tparametercollection(parameters)
            param_structure = self._determine_parameter_structure(normalized_parameters)
            transformed_operation = transform_paramstyle(
                operation, normalized_parameters, param_structure
            )
            prepared_operation, prepared_params = self._prepare_native_parameters(
                transformed_operation, normalized_parameters, param_structure
            )

        self._check_not_closed()
        self._close_and_clear_active_result_set()
        self.thrift_backend.execute_command(
            operation=prepared_operation,
            session_handle=self.connection._session_handle,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            lz4_compression=self.connection.lz4_compression,
            cursor=self,
            use_cloud_fetch=self.connection.use_cloud_fetch,
            parameters=prepared_params,
            async_op=True,
            enforce_embedded_schema_correctness=enforce_embedded_schema_correctness,
        )

        return self

    def get_query_state(self) -> "TOperationState":
        """
        Get the state of the async executing query or basically poll the status of the query

        :return:
        """
        self._check_not_closed()
        return self.thrift_backend.get_query_state(self.active_op_handle)

    def is_query_pending(self):
        """
        Checks whether the async executing query is in pending state or not

        :return:
        """
        operation_state = self.get_query_state()

        return not operation_state or operation_state in [
            ttypes.TOperationState.RUNNING_STATE,
            ttypes.TOperationState.PENDING_STATE,
        ]

    def get_async_execution_result(self):
        """

        Checks for the status of the async executing query and fetches the result if the query is finished
        Otherwise it will keep polling the status of the query till there is a Not pending state
        :return:
        """
        self._check_not_closed()

        while self.is_query_pending():
            # Poll after some default time
            time.sleep(self.ASYNC_DEFAULT_POLLING_INTERVAL)

        operation_state = self.get_query_state()
        if operation_state == ttypes.TOperationState.FINISHED_STATE:
            execute_response = self.thrift_backend.get_execution_result(
                self.active_op_handle, self
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
        else:
            raise Error(
                f"get_execution_result failed with Operation status {operation_state}"
            )

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
        table_types: Optional[List[str]] = None,
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

    def fetchall_arrow(self) -> "pyarrow.Table":
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchall_arrow()
        else:
            raise Error("There is no active result set")

    def fetchmany_arrow(self, size) -> "pyarrow.Table":
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
        self.active_op_handle = None
        if self.active_result_set:
            self._close_and_clear_active_result_set()

    @property
    def query_id(self) -> Optional[str]:
        """
        This attribute is an identifier of last executed query.

        This attribute will be ``None`` if the cursor has not had an operation
        invoked via the execute method yet, or if cursor was closed.
        """
        if self.active_op_handle is not None:
            return str(UUID(bytes=self.active_op_handle.operationId.guid))
        return None

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


class ResultSet:
    def __init__(
        self,
        connection: Connection,
        execute_response: ExecuteResponse,
        thrift_backend: ThriftBackend,
        result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
        arraysize: int = 10000,
        use_cloud_fetch: bool = True,
    ):
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
        self.lz4_compressed = execute_response.lz4_compressed
        self.arraysize = arraysize
        self.thrift_backend = thrift_backend
        self.description = execute_response.description
        self._arrow_schema_bytes = execute_response.arrow_schema_bytes
        self._next_row_index = 0
        self._use_cloud_fetch = use_cloud_fetch

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
        # At initialization or if the server does not have cloud fetch result links available
        results, has_more_rows = self.thrift_backend.fetch_results(
            op_handle=self.command_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            expected_row_start_offset=self._next_row_index,
            lz4_compressed=self.lz4_compressed,
            arrow_schema_bytes=self._arrow_schema_bytes,
            description=self.description,
            use_cloud_fetch=self._use_cloud_fetch,
        )
        self.results = results
        self.has_more_rows = has_more_rows

    def _convert_columnar_table(self, table):
        column_names = [c[0] for c in self.description]
        ResultRow = Row(*column_names)
        result = []
        for row_index in range(table.num_rows):
            curr_row = []
            for col_index in range(table.num_columns):
                curr_row.append(table.get_item(col_index, row_index))
            result.append(ResultRow(*curr_row))

        return result

    def _convert_arrow_table(self, table):
        column_names = [c[0] for c in self.description]
        ResultRow = Row(*column_names)

        if self.connection.disable_pandas is True:
            return [
                ResultRow(*[v.as_py() for v in r]) for r in zip(*table.itercolumns())
            ]

        # Need to use nullable types, as otherwise type can change when there are missing values.
        # See https://arrow.apache.org/docs/python/pandas.html#nullable-types
        # NOTE: This api is epxerimental https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        dtype_mapping = {
            pyarrow.int8(): pandas.Int8Dtype(),
            pyarrow.int16(): pandas.Int16Dtype(),
            pyarrow.int32(): pandas.Int32Dtype(),
            pyarrow.int64(): pandas.Int64Dtype(),
            pyarrow.uint8(): pandas.UInt8Dtype(),
            pyarrow.uint16(): pandas.UInt16Dtype(),
            pyarrow.uint32(): pandas.UInt32Dtype(),
            pyarrow.uint64(): pandas.UInt64Dtype(),
            pyarrow.bool_(): pandas.BooleanDtype(),
            pyarrow.float32(): pandas.Float32Dtype(),
            pyarrow.float64(): pandas.Float64Dtype(),
            pyarrow.string(): pandas.StringDtype(),
        }

        # Need to rename columns, as the to_pandas function cannot handle duplicate column names
        table_renamed = table.rename_columns([str(c) for c in range(table.num_columns)])
        df = table_renamed.to_pandas(
            types_mapper=dtype_mapping.get,
            date_as_object=True,
            timestamp_as_object=True,
        )

        res = df.to_numpy(na_value=None, dtype="object")
        return [ResultRow(*v) for v in res]

    @property
    def rownumber(self):
        return self._next_row_index

    def fetchmany_arrow(self, size: int) -> "pyarrow.Table":
        """
        Fetch the next set of rows of a query result, returning a PyArrow table.

        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError("size argument for fetchmany is %s but must be >= 0", size)
        results = self.results.next_n_rows(size)
        n_remaining_rows = size - results.num_rows
        self._next_row_index += results.num_rows

        while (
            n_remaining_rows > 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            results = pyarrow.concat_tables([results, partial_results])
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        return results

    def merge_columnar(self, result1, result2):
        """
        Function to merge / combining the columnar results into a single result
        :param result1:
        :param result2:
        :return:
        """

        if result1.column_names != result2.column_names:
            raise ValueError("The columns in the results don't match")

        merged_result = [
            result1.column_table[i] + result2.column_table[i]
            for i in range(result1.num_columns)
        ]
        return ColumnTable(merged_result, result1.column_names)

    def fetchmany_columnar(self, size: int):
        """
        Fetch the next set of rows of a query result, returning a Columnar Table.
        An empty sequence is returned when no more rows are available.
        """
        if size < 0:
            raise ValueError("size argument for fetchmany is %s but must be >= 0", size)

        results = self.results.next_n_rows(size)
        n_remaining_rows = size - results.num_rows
        self._next_row_index += results.num_rows

        while (
            n_remaining_rows > 0
            and not self.has_been_closed_server_side
            and self.has_more_rows
        ):
            self._fill_results_buffer()
            partial_results = self.results.next_n_rows(n_remaining_rows)
            results = self.merge_columnar(results, partial_results)
            n_remaining_rows -= partial_results.num_rows
            self._next_row_index += partial_results.num_rows

        return results

    def fetchall_arrow(self) -> "pyarrow.Table":
        """Fetch all (remaining) rows of a query result, returning them as a PyArrow table."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            results = pyarrow.concat_tables([results, partial_results])
            self._next_row_index += partial_results.num_rows

        return results

    def fetchall_columnar(self):
        """Fetch all (remaining) rows of a query result, returning them as a Columnar table."""
        results = self.results.remaining_rows()
        self._next_row_index += results.num_rows

        while not self.has_been_closed_server_side and self.has_more_rows:
            self._fill_results_buffer()
            partial_results = self.results.remaining_rows()
            results = self.merge_columnar(results, partial_results)
            self._next_row_index += partial_results.num_rows

        return results

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        """

        if isinstance(self.results, ColumnQueue):
            res = self._convert_columnar_table(self.fetchmany_columnar(1))
        else:
            res = self._convert_arrow_table(self.fetchmany_arrow(1))

        if len(res) > 0:
            return res[0]
        else:
            return None

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result, returning them as a list of rows.
        """
        if isinstance(self.results, ColumnQueue):
            return self._convert_columnar_table(self.fetchall_columnar())
        else:
            return self._convert_arrow_table(self.fetchall_arrow())

    def fetchmany(self, size: int) -> List[Row]:
        """
        Fetch the next set of rows of a query result, returning a list of rows.

        An empty sequence is returned when no more rows are available.
        """
        if isinstance(self.results, ColumnQueue):
            return self._convert_columnar_table(self.fetchmany_columnar(size))
        else:
            return self._convert_arrow_table(self.fetchmany_arrow(size))

    def close(self) -> None:
        """
        Close the cursor.

        If the connection has not been closed, and the cursor has not already
        been closed on the server for some other reason, issue a request to the server to close it.
        """
        try:
            if (
                self.op_state != self.thrift_backend.CLOSED_OP_STATE
                and not self.has_been_closed_server_side
                and self.connection.open
            ):
                self.thrift_backend.close_command(self.command_id)
        except RequestError as e:
            if isinstance(e.args[1], CursorAlreadyClosedError):
                logger.info("Operation was canceled by a prior request")
        finally:
            self.has_been_closed_server_side = True
            self.op_state = self.thrift_backend.CLOSED_OP_STATE

    @staticmethod
    def _get_schema_description(table_schema_message):
        """
        Takes a TableSchema message and returns a description 7-tuple as specified by PEP-249
        """

        def map_col_type(type_):
            if type_.startswith("decimal"):
                return "decimal"
            else:
                return type_

        return [
            (column.name, map_col_type(column.datatype), None, None, None, None, None)
            for column in table_schema_message.columns
        ]
