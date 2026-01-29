import time
from typing import Dict, Tuple, List, Optional, Any, Union, Sequence, BinaryIO
import pandas

try:
    import pyarrow
except ImportError:
    pyarrow = None
import json
import os
import decimal
from urllib.parse import urlparse
from uuid import UUID

from databricks.sql import __version__
from databricks.sql import *
from databricks.sql.exc import (
    OperationalError,
    SessionAlreadyClosedError,
    CursorAlreadyClosedError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
    TransactionError,
    DatabaseError,
)

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.utils import (
    ParamEscaper,
    inject_parameters,
    transform_paramstyle,
    ColumnTable,
    ColumnQueue,
    build_client_context,
    get_session_config_value,
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

from databricks.sql.result_set import ResultSet, ThriftResultSet
from databricks.sql.types import Row, SSLOptions
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.experimental.oauth_persistence import OAuthPersistence
from databricks.sql.session import Session
from databricks.sql.backend.types import CommandId, BackendType, CommandState, SessionId

from databricks.sql.auth.common import ClientContext
from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.common.http import HttpMethod

from databricks.sql.thrift_api.TCLIService.ttypes import (
    TOpenSessionResp,
    TSparkParameter,
    TOperationState,
)
from databricks.sql.telemetry.telemetry_client import (
    TelemetryHelper,
    TelemetryClientFactory,
)
from databricks.sql.telemetry.models.enums import DatabricksClientType
from databricks.sql.telemetry.models.event import (
    DriverConnectionParameters,
    HostDetails,
)
from databricks.sql.telemetry.latency_logger import log_latency
from databricks.sql.telemetry.models.enums import StatementType

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

# Transaction isolation level constants (extension to PEP 249)
TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ = "REPEATABLE_READ"


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
        ignore_transactions: bool = True,
        **kwargs,
    ) -> None:
        """
        Connect to a Databricks SQL endpoint or a Databricks cluster.

        Parameters:
            :param use_sea: `bool`, optional (default is False)
                Use the SEA backend instead of the Thrift backend.
            :param use_hybrid_disposition: `bool`, optional (default is False)
                Use the hybrid disposition instead of the inline disposition.
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
            :param enable_metric_view_metadata: `bool`, optional (default is False)
                When True, enables metric view metadata support by setting the
                spark.sql.thriftserver.metadata.metricview.enabled session configuration.
                This allows
                1. cursor.tables() to return METRIC_VIEW table type
                2. cursor.columns() to return "measure" column type
            :param fetch_autocommit_from_server: `bool`, optional (default is False)
                When True, the connection.autocommit property queries the server for current state
                using SET AUTOCOMMIT instead of returning cached value.
                Set to True if autocommit might be changed by external means (e.g., external SQL commands).
                When False (default), uses cached state for better performance.
            :param ignore_transactions: `bool`, optional (default is True)
                When True, transaction-related operations behave as follows:
                - commit(): no-op (does nothing)
                - rollback(): raises NotSupportedError
                - autocommit setter: no-op (does nothing)
                When False, transaction operations execute normally.
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

        logger.debug(
            "Connection.__init__(server_hostname=%s, http_path=%s)",
            server_hostname,
            http_path,
        )

        if access_token:
            access_token_kv = {"access_token": access_token}
            kwargs = {**kwargs, **access_token_kv}

        enable_metric_view_metadata = kwargs.get("enable_metric_view_metadata", False)
        if enable_metric_view_metadata:
            if session_configuration is None:
                session_configuration = {}
            session_configuration[
                "spark.sql.thriftserver.metadata.metricview.enabled"
            ] = "true"

        self.disable_pandas = kwargs.get("_disable_pandas", False)
        self.lz4_compression = kwargs.get("enable_query_result_lz4_compression", True)
        self.use_cloud_fetch = kwargs.get("use_cloud_fetch", True)
        self._cursors = []  # type: List[Cursor]
        self.telemetry_batch_size = kwargs.get(
            "telemetry_batch_size", TelemetryClientFactory.DEFAULT_BATCH_SIZE
        )

        client_context = build_client_context(server_hostname, __version__, **kwargs)
        self.http_client = UnifiedHttpClient(client_context)

        try:
            self.session = Session(
                server_hostname,
                http_path,
                self.http_client,
                http_headers,
                session_configuration,
                catalog,
                schema,
                _use_arrow_native_complex_types,
                **kwargs,
            )
            self.session.open()
        except Exception as e:
            # Respect user's telemetry preference even during connection failure
            enable_telemetry = kwargs.get("enable_telemetry", True)
            TelemetryClientFactory.connection_failure_log(
                error_name="Exception",
                error_message=str(e),
                host_url=server_hostname,
                http_path=http_path,
                port=kwargs.get("_port", 443),
                client_context=client_context,
                user_agent=self.session.useragent_header
                if hasattr(self, "session")
                else None,
                enable_telemetry=enable_telemetry,
            )
            raise e

        self.use_inline_params = self._set_use_inline_params_with_warning(
            kwargs.get("use_inline_params", False)
        )
        self.staging_allowed_local_path = kwargs.get("staging_allowed_local_path", None)
        self._fetch_autocommit_from_server = kwargs.get(
            "fetch_autocommit_from_server", False
        )
        self.ignore_transactions = ignore_transactions

        self.force_enable_telemetry = kwargs.get("force_enable_telemetry", False)
        self.enable_telemetry = kwargs.get("enable_telemetry", True)
        self.telemetry_enabled = TelemetryHelper.is_telemetry_enabled(self)

        TelemetryClientFactory.initialize_telemetry_client(
            telemetry_enabled=self.telemetry_enabled,
            session_id_hex=self.get_session_id_hex(),
            auth_provider=self.session.auth_provider,
            host_url=self.session.host,
            batch_size=self.telemetry_batch_size,
            client_context=client_context,
        )

        self._telemetry_client = TelemetryClientFactory.get_telemetry_client(
            host_url=self.session.host
        )

        # Determine proxy usage
        use_proxy = self.http_client.using_proxy()
        proxy_host_info = None
        if (
            use_proxy
            and self.http_client.proxy_uri
            and isinstance(self.http_client.proxy_uri, str)
        ):
            parsed = urlparse(self.http_client.proxy_uri)
            proxy_host_info = HostDetails(
                host_url=parsed.hostname or self.http_client.proxy_uri,
                port=parsed.port or 8080,
            )

        driver_connection_params = DriverConnectionParameters(
            http_path=http_path,
            mode=DatabricksClientType.SEA
            if self.session.use_sea
            else DatabricksClientType.THRIFT,
            host_info=HostDetails(host_url=server_hostname, port=self.session.port),
            auth_mech=TelemetryHelper.get_auth_mechanism(self.session.auth_provider),
            auth_flow=TelemetryHelper.get_auth_flow(self.session.auth_provider),
            socket_timeout=kwargs.get("_socket_timeout", None),
            azure_workspace_resource_id=kwargs.get("azure_workspace_resource_id", None),
            azure_tenant_id=kwargs.get("azure_tenant_id", None),
            use_proxy=use_proxy,
            use_system_proxy=use_proxy,
            proxy_host_info=proxy_host_info,
            use_cf_proxy=False,  # CloudFlare proxy not yet supported in Python
            cf_proxy_host_info=None,  # CloudFlare proxy not yet supported in Python
            non_proxy_hosts=None,
            allow_self_signed_support=kwargs.get("_tls_no_verify", False),
            use_system_trust_store=True,  # Python uses system SSL by default
            enable_arrow=pyarrow is not None,
            enable_direct_results=True,  # Always enabled in Python
            enable_sea_hybrid_results=kwargs.get("use_hybrid_disposition", False),
            http_connection_pool_size=kwargs.get("pool_maxsize", None),
            rows_fetched_per_block=DEFAULT_ARRAY_SIZE,
            async_poll_interval_millis=2000,  # Default polling interval
            support_many_parameters=True,  # Native parameters supported
            enable_complex_datatype_support=_use_arrow_native_complex_types,
            allowed_volume_ingestion_paths=self.staging_allowed_local_path,
            query_tags=get_session_config_value(session_configuration, "query_tags"),
        )

        self._telemetry_client.export_initial_telemetry_log(
            driver_connection_params=driver_connection_params,
            user_agent=self.session.useragent_header,
            session_id=self.get_session_id_hex(),
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
        """Get the raw session ID (backend-specific)"""
        return self.session.guid

    def get_session_id_hex(self):
        """Get the session ID in hex format"""
        return self.session.guid_hex

    @staticmethod
    def server_parameterized_queries_enabled(protocolVersion):
        """Check if parameterized queries are enabled for the given protocol version"""
        return Session.server_parameterized_queries_enabled(protocolVersion)

    @property
    def protocol_version(self):
        """Get the protocol version from the Session object"""
        return self.session.protocol_version

    @staticmethod
    def get_protocol_version(openSessionResp: TOpenSessionResp):
        """Get the protocol version from the OpenSessionResp object"""
        properties = (
            {"serverProtocolVersion": openSessionResp.serverProtocolVersion}
            if openSessionResp.serverProtocolVersion
            else {}
        )
        session_id = SessionId.from_thrift_handle(
            openSessionResp.sessionHandle, properties
        )
        return Session.get_protocol_version(session_id)

    @property
    def open(self) -> bool:
        """Return whether the connection is open by checking if the session is open."""
        return self.session.is_open

    def cursor(
        self,
        arraysize: int = DEFAULT_ARRAY_SIZE,
        buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
        row_limit: Optional[int] = None,
    ) -> "Cursor":
        """
        Args:
            arraysize: The maximum number of rows in direct results.
            buffer_size_bytes: The maximum number of bytes in direct results.
            row_limit: The maximum number of rows in the result.

        Return a new Cursor object using the connection.

        Will throw an Error if the connection has been closed.
        """
        if not self.open:
            raise InterfaceError(
                "Cannot create cursor from closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        cursor = Cursor(
            self,
            self.session.backend,
            arraysize=arraysize,
            result_buffer_size_bytes=buffer_size_bytes,
            row_limit=row_limit,
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

        try:
            self.session.close()
        except Exception as e:
            logger.error(f"Attempt to close session raised a local exception: {e}")

        TelemetryClientFactory.close(host_url=self.session.host)

        # Close HTTP client that was created by this connection
        if self.http_client:
            self.http_client.close()

    @property
    def autocommit(self) -> bool:
        """
        Get auto-commit mode for this connection.

        Extension to PEP 249. Returns cached value by default.
        If fetch_autocommit_from_server=True was set during connection,
        queries server for current state.

        Returns:
            bool: True if auto-commit is enabled, False otherwise

        Raises:
            InterfaceError: If connection is closed
            TransactionError: If fetch_autocommit_from_server=True and query fails
        """
        if not self.open:
            raise InterfaceError(
                "Cannot get autocommit on closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        if self._fetch_autocommit_from_server:
            return self._fetch_autocommit_state_from_server()

        return self.session.get_autocommit()

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """
        Set auto-commit mode for this connection.

        Extension to PEP 249. Executes SET AUTOCOMMIT command on server.

        Args:
            value: True to enable auto-commit, False to disable

        When ignore_transactions is True:
        - This method is a no-op (does nothing)

        Raises:
            InterfaceError: If connection is closed
            TransactionError: If server rejects the change
        """
        # No-op when ignore_transactions is True
        if self.ignore_transactions:
            return

        if not self.open:
            raise InterfaceError(
                "Cannot set autocommit on closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        # Create internal cursor for transaction control
        cursor = None
        try:
            cursor = self.cursor()
            sql = f"SET AUTOCOMMIT = {'TRUE' if value else 'FALSE'}"
            cursor.execute(sql)

            # Update cached state on success
            self.session.set_autocommit(value)

        except DatabaseError as e:
            # Wrap in TransactionError with context
            raise TransactionError(
                f"Failed to set autocommit to {value}: {e.message}",
                context={
                    **e.context,
                    "operation": "set_autocommit",
                    "autocommit_value": value,
                },
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            ) from e
        finally:
            if cursor:
                cursor.close()

    def _fetch_autocommit_state_from_server(self) -> bool:
        """
        Query server for current autocommit state using SET AUTOCOMMIT.

        Returns:
            bool: Server's autocommit state

        Raises:
            TransactionError: If query fails
        """
        cursor = None
        try:
            cursor = self.cursor()
            cursor.execute("SET AUTOCOMMIT")

            # Fetch result: should return row with value column
            result = cursor.fetchone()
            if result is None:
                raise TransactionError(
                    "No result returned from SET AUTOCOMMIT query",
                    context={"operation": "fetch_autocommit"},
                    host_url=self.session.host,
                    session_id_hex=self.get_session_id_hex(),
                )

            # Parse value (first column should be "true" or "false")
            value_str = str(result[0]).lower()
            autocommit_state = value_str == "true"

            # Update cache
            self.session.set_autocommit(autocommit_state)

            return autocommit_state

        except TransactionError:
            # Re-raise TransactionError as-is
            raise
        except DatabaseError as e:
            # Wrap other DatabaseErrors
            raise TransactionError(
                f"Failed to fetch autocommit state from server: {e.message}",
                context={**e.context, "operation": "fetch_autocommit"},
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            ) from e
        finally:
            if cursor:
                cursor.close()

    def commit(self) -> None:
        """
        Commit the current transaction.

        Per PEP 249. Should be called only when autocommit is disabled.

        When autocommit is False:
        - Commits the current transaction
        - Server automatically starts new transaction

        When autocommit is True:
        - Server may throw error if no active transaction

        When ignore_transactions is True:
        - This method is a no-op (does nothing)

        Raises:
            InterfaceError: If connection is closed
            TransactionError: If commit fails (e.g., no active transaction)
        """
        # No-op when ignore_transactions is True
        if self.ignore_transactions:
            return

        if not self.open:
            raise InterfaceError(
                "Cannot commit on closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        cursor = None
        try:
            cursor = self.cursor()
            cursor.execute("COMMIT")

        except DatabaseError as e:
            raise TransactionError(
                f"Failed to commit transaction: {e.message}",
                context={**e.context, "operation": "commit"},
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            ) from e
        finally:
            if cursor:
                cursor.close()

    def rollback(self) -> None:
        """
        Rollback the current transaction.

        Per PEP 249. Should be called only when autocommit is disabled.

        When autocommit is False:
        - Rolls back the current transaction
        - Server automatically starts new transaction

        When autocommit is True:
        - ROLLBACK is forgiving (no-op, doesn't throw exception)

        When ignore_transactions is True:
        - Raises NotSupportedError

        Note: ROLLBACK is safe to call even without active transaction.

        Raises:
            InterfaceError: If connection is closed
            NotSupportedError: If ignore_transactions is True
            TransactionError: If rollback fails
        """
        # Raise NotSupportedError when ignore_transactions is True
        if self.ignore_transactions:
            raise NotSupportedError(
                "Transactions are not supported on Databricks",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        if not self.open:
            raise InterfaceError(
                "Cannot rollback on closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        cursor = None
        try:
            cursor = self.cursor()
            cursor.execute("ROLLBACK")

        except DatabaseError as e:
            raise TransactionError(
                f"Failed to rollback transaction: {e.message}",
                context={**e.context, "operation": "rollback"},
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            ) from e
        finally:
            if cursor:
                cursor.close()

    def get_transaction_isolation(self) -> str:
        """
        Get the transaction isolation level.

        Extension to PEP 249.

        Databricks supports REPEATABLE_READ isolation level (Snapshot Isolation),
        which is the default and only supported level.

        Returns:
            str: "REPEATABLE_READ" - the transaction isolation level constant

        Raises:
            InterfaceError: If connection is closed
        """
        if not self.open:
            raise InterfaceError(
                "Cannot get transaction isolation on closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        return TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ

    def set_transaction_isolation(self, level: str) -> None:
        """
        Set transaction isolation level.

        Extension to PEP 249.

        Databricks supports only REPEATABLE_READ isolation level (Snapshot Isolation).
        This method validates that the requested level is supported but does not
        execute any SQL, as REPEATABLE_READ is the default server behavior.

        Args:
            level: Isolation level. Must be "REPEATABLE_READ" or "REPEATABLE READ"
                   (case-insensitive, underscores and spaces are interchangeable)

        Raises:
            InterfaceError: If connection is closed
            NotSupportedError: If isolation level not supported
        """
        if not self.open:
            raise InterfaceError(
                "Cannot set transaction isolation on closed connection",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )

        # Normalize and validate isolation level
        normalized_level = level.upper().replace("_", " ")

        if normalized_level != TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ.replace(
            "_", " "
        ):
            raise NotSupportedError(
                f"Setting transaction isolation level '{level}' is not supported. "
                f"Only {TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ} is supported.",
                host_url=self.session.host,
                session_id_hex=self.get_session_id_hex(),
            )


class Cursor:
    def __init__(
        self,
        connection: Connection,
        backend: DatabricksClient,
        result_buffer_size_bytes: int = DEFAULT_RESULT_BUFFER_SIZE_BYTES,
        arraysize: int = DEFAULT_ARRAY_SIZE,
        row_limit: Optional[int] = None,
    ) -> None:
        """
        These objects represent a database cursor, which is used to manage the context of a fetch
        operation.

        Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
        visible by other cursors or connections.
        """

        self.connection: Connection = connection

        self.rowcount: int = -1  # Return -1 as this is not supported
        self.buffer_size_bytes: int = result_buffer_size_bytes
        self.active_result_set: Union[ResultSet, None] = None
        self.arraysize: int = arraysize
        self.row_limit: Optional[int] = row_limit
        # Note that Cursor closed => active result set closed, but not vice versa
        self.open: bool = True
        self.executing_command_id: Optional[CommandId] = None
        self.backend: DatabricksClient = backend
        self.active_command_id: Optional[CommandId] = None
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
            raise ProgrammingError(
                "There is no active result set",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

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
            raise InterfaceError(
                "Attempting operation on closed cursor",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

    def _handle_staging_operation(
        self,
        staging_allowed_local_path: Union[None, str, List[str]],
        input_stream: Optional[BinaryIO] = None,
    ):
        """Fetch the HTTP request instruction from a staging ingestion command
        and call the designated handler.

        Raise an exception if localFile is specified by the server but the localFile
        is not descended from staging_allowed_local_path.
        """

        assert self.active_result_set is not None
        row = self.active_result_set.fetchone()
        assert row is not None

        # May be real headers, or could be json string
        headers = (
            json.loads(row.headers) if isinstance(row.headers, str) else row.headers
        )
        headers = dict(headers) if headers else {}

        # Handle __input_stream__ token for PUT operations
        if (
            row.operation == "PUT"
            and getattr(row, "localFile", None) == "__input_stream__"
        ):
            return self._handle_staging_put_stream(
                presigned_url=row.presignedUrl,
                stream=input_stream,
                headers=headers,
            )

        # For non-streaming operations, validate staging_allowed_local_path
        if isinstance(staging_allowed_local_path, type(str())):
            _staging_allowed_local_paths = [staging_allowed_local_path]
        elif isinstance(staging_allowed_local_path, type(list())):
            _staging_allowed_local_paths = staging_allowed_local_path
        else:
            raise ProgrammingError(
                "You must provide at least one staging_allowed_local_path when initialising a connection to perform ingestion commands",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

        abs_staging_allowed_local_paths = [
            os.path.abspath(i) for i in _staging_allowed_local_paths
        ]

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
                raise ProgrammingError(
                    "Local file operations are restricted to paths within the configured staging_allowed_local_path",
                    host_url=self.connection.session.host,
                    session_id_hex=self.connection.get_session_id_hex(),
                )

        handler_args = {
            "presigned_url": row.presignedUrl,
            "local_file": abs_localFile,
            "headers": headers,
        }

        logger.debug(
            "Attempting staging operation indicated by server: %s - %s",
            row.operation,
            getattr(row, "localFile", ""),
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
            raise ProgrammingError(
                f"Operation {row.operation} is not supported. "
                + "Supported operations are GET, PUT, and REMOVE",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

    @log_latency(StatementType.SQL)
    def _handle_staging_put(
        self, presigned_url: str, local_file: str, headers: Optional[dict] = None
    ):
        """Make an HTTP PUT request

        Raise an exception if request fails. Returns no data.
        """

        if local_file is None:
            raise ProgrammingError(
                "Cannot perform PUT without specifying a local_file",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

        with open(local_file, "rb") as fh:
            r = self.connection.http_client.request(
                HttpMethod.PUT, presigned_url, body=fh.read(), headers=headers
            )

        self._handle_staging_http_response(r)

    def _handle_staging_http_response(self, r):

        # fmt: off
        # HTTP status codes
        OK = 200
        CREATED = 201
        ACCEPTED = 202
        NO_CONTENT = 204
        # fmt: on

        if r.status not in [OK, CREATED, NO_CONTENT, ACCEPTED]:
            # Decode response data for error message
            error_text = r.data.decode() if r.data else ""
            raise OperationalError(
                f"Staging operation over HTTP was unsuccessful: {r.status}-{error_text}",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

        if r.status == ACCEPTED:
            logger.debug(
                f"Response code {ACCEPTED} from server indicates ingestion command was accepted "
                + "but not yet applied on the server. It's possible this command may fail later."
            )

    @log_latency(StatementType.SQL)
    def _handle_staging_put_stream(
        self,
        presigned_url: str,
        stream: BinaryIO,
        headers: dict = {},
    ) -> None:
        """Handle PUT operation with streaming data.

        Args:
            presigned_url: The presigned URL for upload
            stream: Binary stream to upload
            headers: HTTP headers

        Raises:
            ProgrammingError: If no input stream is provided
            OperationalError: If the upload fails
        """

        if not stream:
            raise ProgrammingError(
                "No input stream provided for streaming operation",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

        r = self.connection.http_client.request(
            HttpMethod.PUT, presigned_url, body=stream.read(), headers=headers
        )

        self._handle_staging_http_response(r)

    @log_latency(StatementType.SQL)
    def _handle_staging_get(
        self, local_file: str, presigned_url: str, headers: Optional[dict] = None
    ):
        """Make an HTTP GET request, create a local file with the received data

        Raise an exception if request fails. Returns no data.
        """

        if local_file is None:
            raise ProgrammingError(
                "Cannot perform GET without specifying a local_file",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

        r = self.connection.http_client.request(
            HttpMethod.GET, presigned_url, headers=headers
        )

        # response.ok verifies the status code is not between 400-600.
        # Any 2xx or 3xx will evaluate r.ok == True
        if r.status >= 400:
            # Decode response data for error message
            error_text = r.data.decode() if r.data else ""
            raise OperationalError(
                f"Staging operation over HTTP was unsuccessful: {r.status}-{error_text}",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

        with open(local_file, "wb") as fp:
            fp.write(r.data)

    @log_latency(StatementType.SQL)
    def _handle_staging_remove(
        self, presigned_url: str, headers: Optional[dict] = None
    ):
        """Make an HTTP DELETE request to the presigned_url"""

        r = self.connection.http_client.request(
            HttpMethod.DELETE, presigned_url, headers=headers
        )

        if r.status >= 400:
            # Decode response data for error message
            error_text = r.data.decode() if r.data else ""
            raise OperationalError(
                f"Staging operation over HTTP was unsuccessful: {r.status}-{error_text}",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

    @log_latency(StatementType.QUERY)
    def execute(
        self,
        operation: str,
        parameters: Optional[TParameterCollection] = None,
        enforce_embedded_schema_correctness=False,
        input_stream: Optional[BinaryIO] = None,
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

        logger.debug(
            "Cursor.execute(operation=%s, parameters=%s)", operation, parameters
        )

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
        self.active_result_set = self.backend.execute_command(
            operation=prepared_operation,
            session_id=self.connection.session.session_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            lz4_compression=self.connection.lz4_compression,
            cursor=self,
            use_cloud_fetch=self.connection.use_cloud_fetch,
            parameters=prepared_params,
            async_op=False,
            enforce_embedded_schema_correctness=enforce_embedded_schema_correctness,
            row_limit=self.row_limit,
        )

        if self.active_result_set and self.active_result_set.is_staging_operation:
            self._handle_staging_operation(
                staging_allowed_local_path=self.connection.staging_allowed_local_path,
                input_stream=input_stream,
            )

        return self

    @log_latency(StatementType.QUERY)
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
        self.backend.execute_command(
            operation=prepared_operation,
            session_id=self.connection.session.session_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            lz4_compression=self.connection.lz4_compression,
            cursor=self,
            use_cloud_fetch=self.connection.use_cloud_fetch,
            parameters=prepared_params,
            async_op=True,
            enforce_embedded_schema_correctness=enforce_embedded_schema_correctness,
            row_limit=self.row_limit,
        )

        return self

    def get_query_state(self) -> CommandState:
        """
        Get the state of the async executing query or basically poll the status of the query

        :return:
        """
        self._check_not_closed()
        if self.active_command_id is None:
            raise Error("No active command to get state for")
        return self.backend.get_query_state(self.active_command_id)

    def is_query_pending(self):
        """
        Checks whether the async executing query is in pending state or not

        :return:
        """
        operation_state = self.get_query_state()
        return operation_state in [CommandState.PENDING, CommandState.RUNNING]

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
        if operation_state == CommandState.SUCCEEDED:
            self.active_result_set = self.backend.get_execution_result(
                self.active_command_id, self
            )

            if self.active_result_set and self.active_result_set.is_staging_operation:
                self._handle_staging_operation(
                    staging_allowed_local_path=self.connection.staging_allowed_local_path
                )

            return self
        else:
            raise OperationalError(
                f"get_execution_result failed with Operation status {operation_state}",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
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

    @log_latency(StatementType.METADATA)
    def catalogs(self) -> "Cursor":
        """
        Get all available catalogs.

        :returns self
        """
        self._check_not_closed()
        self._close_and_clear_active_result_set()
        self.active_result_set = self.backend.get_catalogs(
            session_id=self.connection.session.session_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
        )
        return self

    @log_latency(StatementType.METADATA)
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
        self.active_result_set = self.backend.get_schemas(
            session_id=self.connection.session.session_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
        return self

    @log_latency(StatementType.METADATA)
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

        self.active_result_set = self.backend.get_tables(
            session_id=self.connection.session.session_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            table_types=table_types,
        )
        return self

    @log_latency(StatementType.METADATA)
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

        self.active_result_set = self.backend.get_columns(
            session_id=self.connection.session.session_id,
            max_rows=self.arraysize,
            max_bytes=self.buffer_size_bytes,
            cursor=self,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
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
            raise ProgrammingError(
                "There is no active result set",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

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
            raise ProgrammingError(
                "There is no active result set",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

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
            raise ProgrammingError(
                "There is no active result set",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

    def fetchall_arrow(self) -> "pyarrow.Table":
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchall_arrow()
        else:
            raise ProgrammingError(
                "There is no active result set",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

    def fetchmany_arrow(self, size) -> "pyarrow.Table":
        self._check_not_closed()
        if self.active_result_set:
            return self.active_result_set.fetchmany_arrow(size)
        else:
            raise ProgrammingError(
                "There is no active result set",
                host_url=self.connection.session.host,
                session_id_hex=self.connection.get_session_id_hex(),
            )

    def cancel(self) -> None:
        """
        Cancel a running command.

        The command should be closed to free resources from the server.
        This method can be called from another thread.
        """
        if self.active_command_id is not None:
            self.backend.cancel_command(self.active_command_id)
        else:
            logger.warning(
                "Attempting to cancel a command, but there is no "
                "currently executing command"
            )

    def close(self) -> None:
        """Close cursor"""
        self.open = False
        self.active_command_id = None
        if self.active_result_set:
            self._close_and_clear_active_result_set()

    @property
    def query_id(self) -> Optional[str]:
        """
        This attribute is an identifier of last executed query.

        This attribute will be ``None`` if the cursor has not had an operation
        invoked via the execute method yet, or if cursor was closed.
        """
        if self.active_command_id is not None:
            return self.active_command_id.to_hex_guid()
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
