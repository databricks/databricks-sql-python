import logging
import re
from typing import Dict, Tuple, List, Optional, Any, Type

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.auth.common import ClientContext
from databricks.sql.exc import SessionAlreadyClosedError, DatabaseError, RequestError
from databricks.sql import __version__
from databricks.sql import USER_AGENT_NAME
from databricks.sql.backend.thrift_backend import ThriftDatabricksClient
from databricks.sql.backend.sea.backend import SeaDatabricksClient
from databricks.sql.backend.databricks_client import DatabricksClient
from databricks.sql.backend.types import SessionId, BackendType
from databricks.sql.common.unified_http_client import UnifiedHttpClient
from databricks.sql.common.agent import detect as detect_agent

logger = logging.getLogger(__name__)


class Session:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        http_client: UnifiedHttpClient,
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
        self.http_path = http_path

        # Initialize autocommit state (JDBC default is True)
        self._autocommit = True

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

        agent_product = detect_agent()
        if agent_product:
            self.useragent_header += " agent/{}".format(agent_product)

        base_headers = [("User-Agent", self.useragent_header)]
        all_headers = (http_headers or []) + base_headers

        # Extract ?o=<workspaceId> from http_path for SPOG routing.
        # On SPOG hosts, the httpPath contains ?o=<workspaceId> which routes Thrift
        # requests via the URL. For SEA, telemetry, and feature flags (which use
        # separate endpoints), we inject x-databricks-org-id as an HTTP header.
        self._spog_headers = self._extract_spog_headers(http_path, all_headers)
        if self._spog_headers:
            all_headers = all_headers + list(self._spog_headers.items())

        self.ssl_options = SSLOptions(
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

        # Use the provided HTTP client (created in Connection)
        self.http_client = http_client

        # Create auth provider with HTTP client context
        self.auth_provider = get_python_sql_connector_auth_provider(
            server_hostname, http_client=self.http_client, **kwargs
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
        self.use_sea = kwargs.get("use_sea", False)
        self.use_kernel = kwargs.get("use_kernel", False)

        if self.use_kernel and self.use_sea:
            raise ValueError(
                "use_kernel and use_sea are mutually exclusive — pick one."
            )

        if self.use_kernel:
            # Lazy import so the connector doesn't ImportError at
            # startup when the kernel wheel isn't installed — the
            # error surfaces only when a caller actually requests
            # use_kernel=True.
            from databricks.sql.backend.kernel.client import KernelDatabricksClient

            logger.debug("Creating kernel-backed client for use_kernel=True")
            return KernelDatabricksClient(
                server_hostname=server_hostname,
                http_path=http_path,
                http_headers=all_headers,
                auth_provider=auth_provider,
                ssl_options=self.ssl_options,
                http_client=self.http_client,
                catalog=kwargs.get("catalog"),
                schema=kwargs.get("schema"),
                _use_arrow_native_complex_types=_use_arrow_native_complex_types,
            )

        databricks_client_class: Type[DatabricksClient]
        if self.use_sea:
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
            "ssl_options": self.ssl_options,
            "http_client": self.http_client,
            "_use_arrow_native_complex_types": _use_arrow_native_complex_types,
            **kwargs,
        }
        return databricks_client_class(**common_args)

    # All-purpose-compute Thrift http_path:
    # [/]sql/protocolv1/o/<workspace-id>/<cluster-id>[/...][?...]
    _CLUSTER_PATH_ORG_ID_RE = re.compile(r"(?:^|/)sql/protocolv1/o/(\d+)/[^/?]+")

    @staticmethod
    def _extract_spog_headers(http_path, existing_headers):
        """Extract the workspace ID from http_path for SPOG routing and return it
        as an ``x-databricks-org-id`` header dict.

        Two sources are inspected, in priority order:
          1. ``?o=<workspace-id>`` query parameter in http_path (warehouse paths
             typically encode the workspace this way on SPOG).
          2. ``/sql/protocolv1/o/<workspace-id>/<cluster-id>`` path segment
             (all-purpose compute paths embed the workspace in the path itself).

        An explicit ``x-databricks-org-id`` already set by the caller wins over
        both. Returns an empty dict when no workspace ID can be determined.

        On SPOG (Custom URL) hosts this header is required for non-Thrift
        endpoints — telemetry, feature flags, SEA — to be routed to the right
        workspace. Without it, PoPP falls back to default routing and
        workspace-scoped requests are redirected to ``/login``.
        """
        if not http_path:
            return {}

        # Caller already set the header — never override.
        if any(k == "x-databricks-org-id" for k, _ in existing_headers):
            logger.debug(
                "SPOG header extraction: x-databricks-org-id already set by caller, "
                "not extracting from http_path"
            )
            return {}

        org_id = None
        source = None

        if "?" in http_path:
            from urllib.parse import parse_qs

            query_string = http_path.split("?", 1)[1]
            params = parse_qs(query_string)
            value = params.get("o", [None])[0]
            if value:
                org_id = value
                source = "?o= in http_path"

        if org_id is None:
            cluster_match = Session._CLUSTER_PATH_ORG_ID_RE.search(http_path)
            if cluster_match:
                org_id = cluster_match.group(1)
                source = "cluster path segment"

        if org_id is None:
            logger.debug(
                "SPOG header extraction: no workspace ID found in http_path, "
                "skipping x-databricks-org-id injection"
            )
            return {}

        logger.debug(
            "SPOG header extraction: injecting x-databricks-org-id=%s (extracted from %s)",
            org_id,
            source,
        )
        return {"x-databricks-org-id": org_id}

    def get_spog_headers(self):
        """Returns SPOG routing headers (x-databricks-org-id) if ?o= was in http_path."""
        return dict(self._spog_headers)

    def open(self):
        self._session_id = self.backend.open_session(
            session_configuration=self.session_configuration,
            catalog=self.catalog,
            schema=self.schema,
        )

        self.protocol_version = self.get_protocol_version(self._session_id)
        self.is_open = True
        logger.info("Successfully opened session %s", str(self.guid_hex))

    @staticmethod
    def get_protocol_version(session_id: SessionId):
        return session_id.protocol_version

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
    def guid(self) -> Any:
        """Get the raw session ID (backend-specific)"""
        return self._session_id.guid

    @property
    def guid_hex(self) -> str:
        """Get the session ID in hex format"""
        return self._session_id.hex_guid

    def get_autocommit(self) -> bool:
        """
        Get the cached autocommit state for this session.

        Returns:
            bool: True if autocommit is enabled, False otherwise
        """
        return self._autocommit

    def set_autocommit(self, value: bool) -> None:
        """
        Update the cached autocommit state for this session.

        Args:
            value: True to cache autocommit as enabled, False as disabled
        """
        self._autocommit = value

    def close(self) -> None:
        """Close the underlying session."""
        logger.info("Closing session %s", self.guid_hex)
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
                    "Attempted to close session that was already closed: %s", e
                )
            else:
                logger.warning(
                    "Attempt to close session raised an exception at the server: %s", e
                )
        except Exception as e:
            logger.error("Attempt to close session raised a local exception: %s", e)

        self.is_open = False
