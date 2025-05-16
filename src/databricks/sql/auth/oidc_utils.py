import logging
import requests
from typing import Optional

from databricks.sql.auth.endpoint import (
    get_oauth_endpoints,
    infer_cloud_from_host,
)

logger = logging.getLogger(__name__)


class OIDCDiscoveryUtil:
    """
    Utility class for OIDC discovery operations.

    This class handles discovery of OIDC endpoints through standard
    discovery mechanisms, with fallback to default endpoints if needed.
    """

    # Standard token endpoint path for Databricks workspaces
    DEFAULT_TOKEN_PATH = "oidc/v1/token"

    @staticmethod
    def discover_token_endpoint(hostname: str) -> str:
        """
        Get the token endpoint for the given Databricks hostname.

        For Databricks workspaces, the token endpoint is always at host/oidc/v1/token.

        Args:
            hostname: The hostname to get token endpoint for

        Returns:
            str: The token endpoint URL
        """
        # Format the hostname and return the standard endpoint
        hostname = OIDCDiscoveryUtil.format_hostname(hostname)
        token_endpoint = f"{hostname}{OIDCDiscoveryUtil.DEFAULT_TOKEN_PATH}"
        logger.info(f"Using token endpoint: {token_endpoint}")
        return token_endpoint

    @staticmethod
    def format_hostname(hostname: str) -> str:
        """
        Format hostname to ensure it has proper https:// prefix and trailing slash.

        Args:
            hostname: The hostname to format

        Returns:
            str: The formatted hostname
        """
        if not hostname.startswith("https://"):
            hostname = f"https://{hostname}"
        if not hostname.endswith("/"):
            hostname = f"{hostname}/"
        return hostname
