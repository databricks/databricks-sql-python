import logging
import re
from typing import Dict, Tuple, List, Optional, Any, Type

from databricks.sql.thrift_api.TCLIService import ttypes
from databricks.sql.types import SSLOptions
from databricks.sql.auth.auth import get_python_sql_connector_auth_provider
from databricks.sql.auth.authenticators import AccessTokenAuthProvider
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

        # Extract workspace context from http_path for SPOG routing.
        # On SPOG hosts, the http_path can contain either ?o=<workspaceId> or an
        # all-purpose-compute /o/<workspaceId>/ path segment. For SEA, telemetry,
        # and feature flags, we inject x-databricks-org-id as an HTTP header.
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

        # Create auth provider with HTTP client context.
        #
        # On the kernel path the kernel owns the entire auth lifecycle
        # (it acquires/refreshes OAuth tokens itself from the raw
        # credentials — see the kernel auth bridge). We must NOT build
        # the connector's own provider here: for OAuth it would eagerly
        # run the U2M browser flow / M2M token exchange at connect()
        # time (``get_auth_provider`` invokes ``_initial_get_token`` in
        # the provider constructor), racing — and conflicting with — the
        # kernel's auth before ``use_kernel`` is even consulted.
        #
        # So for ``use_kernel`` we hand the bridge only a minimal PAT
        # provider when an ``access_token`` is present, and ``None``
        # otherwise (OAuth M2M/U2M resolve purely from the raw kwargs
        # the bridge reads). The Thrift / SEA backends are unchanged.
        if kwargs.get("use_kernel", False):
            access_token = kwargs.get("access_token")
            self.auth_provider = (
                AccessTokenAuthProvider(access_token) if access_token else None
            )
        else:
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
            # Forward the raw auth-relevant connect() kwargs so the
            # kernel auth bridge can build OAuth kwargs from the
            # original credentials. On this path we intentionally did
            # NOT build the connector's own OAuth provider (see __init__
            # above), so these raw kwargs are the only source of the
            # OAuth client id/secret. These are kernel-only; the Thrift
            # / SEA backends are unaffected.
            kernel_auth_options = {
                "auth_type": kwargs.get("auth_type"),
                "oauth_client_id": kwargs.get("oauth_client_id"),
                "oauth_client_secret": kwargs.get("oauth_client_secret"),
                "oauth_redirect_port": kwargs.get("oauth_redirect_port"),
                "oauth_scopes": kwargs.get("oauth_scopes"),
                "credentials_provider": kwargs.get("credentials_provider"),
            }
            # Forward the connector's retry-tuning kwargs so the kernel's
            # own retry policy honours them (the kernel owns the retry
            # loop on this path). Only the keys with a kernel counterpart
            # are passed; `_retry_delay_default` is intentionally omitted
            # (the kernel's no-Retry-After backoff is exponential from
            # its min-wait, so a flat default delay has no equivalent).
            # Kernel-only; Thrift / SEA are unaffected.
            kernel_retry_options = {
                "retry_delay_min": kwargs.get("_retry_delay_min"),
                "retry_delay_max": kwargs.get("_retry_delay_max"),
                "retry_stop_after_attempts_count": kwargs.get(
                    "_retry_stop_after_attempts_count"
                ),
                "retry_stop_after_attempts_duration": kwargs.get(
                    "_retry_stop_after_attempts_duration"
                ),
            }
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
                auth_options=kernel_auth_options,
                retry_options=kernel_retry_options,
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
    _ORG_ID_RE = re.compile(r"^[0-9]+$")
    _CLUSTER_PATH_ORG_ID_RE = re.compile(r"^/?sql/protocolv1/o/([0-9]+)/[^/?]+")

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

        # Caller already set the header; never override. Header names are case-insensitive.
        if any(k.lower() == "x-databricks-org-id" for k, _ in existing_headers):
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
            if value and Session._ORG_ID_RE.fullmatch(value):
                org_id = value
                source = "?o= in http_path"

        if org_id is None:
            cluster_match = Session._CLUSTER_PATH_ORG_ID_RE.match(http_path)
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
        """Returns extracted SPOG routing headers (x-databricks-org-id), if any."""
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
