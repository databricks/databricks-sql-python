import logging
from typing import Dict, Tuple, List, Optional, Any, Type

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.exc import SessionAlreadyClosedError, DatabaseError, RequestError
from databricks.sql import __version__
from databricks.sql import USER_AGENT_NAME
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import SessionId, BackendType

logger = logging.getLogger(__name__)


class Session:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        http_headers: Optional[List[Tuple[str, str]]] = None,
        session_configuration: Optional[Dict[str, Any]] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        _use_arrow_native_complex_types: Optional[bool] = True,
        **kwargs,
    ) -> None:
        """
        Create a session to a Databricks SQL endpoint or a Databricks cluster.

        This class handles all session-related behavior and communication with the backend.
        """

        self.is_open = False
        self.host = server_hostname
        self.port = kwargs.get("_port", 443)

        self.session_configuration = session_configuration
        self.catalog = catalog
        self.schema = schema

        self.auth_provider = get_python_sql_connector_auth_provider(
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
            self.useragent_header = "{}/{} ({})".format(
                USER_AGENT_NAME, __version__, user_agent_entry
            )
        else:
            self.useragent_header = "{}/{}".format(USER_AGENT_NAME, __version__)

        base_headers = [("User-Agent", self.useragent_header)]
        all_headers = (http_headers or []) + base_headers

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

        self.backend = self._create_backend(
            server_hostname,
            http_path,
            all_headers,
            self.auth_provider,
            _use_arrow_native_complex_types,
            kwargs,
        )

        self.protocol_version = None

    def _create_backend(
        self,
        server_hostname: str,
        http_path: str,
        all_headers: List[Tuple[str, str]],
        auth_provider,
        _use_arrow_native_complex_types: Optional[bool],
        kwargs: dict,
    ) -> DatabricksClient:
        """Create and return the appropriate backend client."""
        use_sea = kwargs.get("use_sea", False)

        databricks_client_class: Type[DatabricksClient]
        if use_sea:
            logger.debug("Creating SEA backend client")
            databricks_client_class = SeaDatabricksClient
        else:
            logger.debug("Creating Thrift backend client")
            databricks_client_class = ThriftDatabricksClient

        common_args = {
            "server_hostname": server_hostname,
            "port": self.port,
            "http_path": http_path,
            "http_headers": all_headers,
            "auth_provider": auth_provider,
            "ssl_options": self._ssl_options,
            "_use_arrow_native_complex_types": _use_arrow_native_complex_types,
            **kwargs,
        }
        return databricks_client_class(**common_args)

    def open(self):
        self._session_id = self.backend.open_session(
            session_configuration=self.session_configuration,
            catalog=self.catalog,
            schema=self.schema,
        )
        self.protocol_version = self.get_protocol_version(self._session_id)
        self.is_open = True
        logger.info("Successfully opened session " + str(self.guid_hex))

    @staticmethod
    def get_protocol_version(session_id: SessionId):
        return session_id.get_protocol_version()

    @staticmethod
    def server_parameterized_queries_enabled(protocolVersion):
        if (
            protocolVersion
            and protocolVersion >= ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8
        ):
            return True
        else:
            return False

    @property
    def session_id(self) -> SessionId:
        """Get the normalized session ID"""
        return self._session_id

    @property
    def guid(self):
        """Get the raw session ID (backend-specific)"""
        return self._session_id.guid

    @property
    def guid_hex(self) -> str:
        """Get the session ID in hex format"""
        return self._session_id.guid_hex

    def close(self) -> None:
        """Close the underlying session."""
        logger.info(f"Closing session {self.guid_hex}")
        if not self.is_open:
            logger.debug("Session appears to have been closed already")
            return

        try:
            self.backend.close_session(self._session_id)
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

        self.is_open = False
