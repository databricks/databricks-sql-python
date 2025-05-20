import logging
from typing import Dict, Tuple, List, Optional, Any

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.exc import SessionAlreadyClosedError, DatabaseError, RequestError
from databricks.sql import __version__
from databricks.sql import USER_AGENT_NAME
from databricks.sql.thrift_backend import ThriftBackend

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
        self.open = False
        self.host = server_hostname
        self.port = kwargs.get("_port", 443)
        
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
        self.open = True
        logger.info("Successfully opened session " + str(self.get_session_id_hex()))

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

    def get_session_handle(self):
        return self._session_handle

    def get_session_id(self):
        return self.thrift_backend.handle_to_id(self._session_handle)

    def get_session_id_hex(self):
        return self.thrift_backend.handle_to_hex_id(self._session_handle)

    def close(self) -> None:
        """Close the underlying session."""
        logger.info(f"Closing session {self.get_session_id_hex()}")
        if not self.open:
            logger.debug("Session appears to have been closed already")
            return

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