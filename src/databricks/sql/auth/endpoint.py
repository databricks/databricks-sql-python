#
# It implements all the cloud specific OAuth configuration/metadata
#
#   Azure:  It uses Databricks internal IdP or Azure AD
#   AWS: It uses Databricks internal IdP
#   GCP: It uses Databricks internal IdP
#
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, List
import os

OIDC_REDIRECTOR_PATH = "oidc"


class OAuthScope:
    OFFLINE_ACCESS = "offline_access"
    SQL = "sql"


class CloudType(Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"


DATABRICKS_AWS_DOMAINS = [
    ".cloud.databricks.com",
    ".cloud.databricks.us",
    ".dev.databricks.com",
]

DATABRICKS_AZURE_DOMAINS = [
    ".azuredatabricks.net",
    ".databricks.azure.cn",
    ".databricks.azure.us",
]
DATABRICKS_GCP_DOMAINS = [".gcp.databricks.com"]

# Domain supported by Databricks InHouse OAuth
DATABRICKS_OAUTH_AZURE_DOMAINS = [".azuredatabricks.net"]


# Infer cloud type from Databricks SQL instance hostname
def infer_cloud_from_host(hostname: str) -> Optional[CloudType]:
    # normalize
    host = hostname.lower().replace("https://", "").split("/")[0]

    if any(e for e in DATABRICKS_AZURE_DOMAINS if host.endswith(e)):
        return CloudType.AZURE
    elif any(e for e in DATABRICKS_AWS_DOMAINS if host.endswith(e)):
        return CloudType.AWS
    elif any(e for e in DATABRICKS_GCP_DOMAINS if host.endswith(e)):
        return CloudType.GCP
    else:
        return None


def is_supported_databricks_oauth_host(hostname: str) -> bool:
    host = hostname.lower().replace("https://", "").split("/")[0]
    domains = (
        DATABRICKS_AWS_DOMAINS + DATABRICKS_GCP_DOMAINS + DATABRICKS_OAUTH_AZURE_DOMAINS
    )
    return any(e for e in domains if host.endswith(e))


def get_databricks_oidc_url(hostname: str):
    maybe_scheme = "https://" if not hostname.startswith("https://") else ""
    maybe_trailing_slash = "/" if not hostname.endswith("/") else ""
    return f"{maybe_scheme}{hostname}{maybe_trailing_slash}{OIDC_REDIRECTOR_PATH}"


class OAuthEndpointCollection(ABC):
    @abstractmethod
    def get_scopes_mapping(self, scopes: List[str]) -> List[str]:
        raise NotImplementedError()

    # Endpoint for oauth2 authorization  e.g https://idp.example.com/oauth2/v2.0/authorize
    @abstractmethod
    def get_authorization_url(self, hostname: str) -> str:
        raise NotImplementedError()

    # Endpoint for well-known openid configuration e.g https://idp.example.com/oauth2/.well-known/openid-configuration
    @abstractmethod
    def get_openid_config_url(self, hostname: str) -> str:
        raise NotImplementedError()


class AzureOAuthEndpointCollection(OAuthEndpointCollection):
    DATATRICKS_AZURE_APP = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

    def get_scopes_mapping(self, scopes: List[str]) -> List[str]:
        # There is no corresponding scopes in Azure, instead, access control will be delegated to Databricks
        tenant_id = os.getenv(
            "DATABRICKS_AZURE_TENANT_ID",
            AzureOAuthEndpointCollection.DATATRICKS_AZURE_APP,
        )
        azure_scope = f"{tenant_id}/user_impersonation"
        mapped_scopes = [azure_scope]
        if OAuthScope.OFFLINE_ACCESS in scopes:
            mapped_scopes.append(OAuthScope.OFFLINE_ACCESS)
        return mapped_scopes

    def get_authorization_url(self, hostname: str):
        # We need get account specific url, which can be redirected by databricks unified oidc endpoint
        return f"{get_databricks_oidc_url(hostname)}/oauth2/v2.0/authorize"

    def get_openid_config_url(self, hostname: str):
        return "https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration"


class InHouseOAuthEndpointCollection(OAuthEndpointCollection):
    def get_scopes_mapping(self, scopes: List[str]) -> List[str]:
        # No scope mapping in AWS
        return scopes.copy()

    def get_authorization_url(self, hostname: str):
        idp_url = get_databricks_oidc_url(hostname)
        return f"{idp_url}/oauth2/v2.0/authorize"

    def get_openid_config_url(self, hostname: str):
        idp_url = get_databricks_oidc_url(hostname)
        return f"{idp_url}/.well-known/oauth-authorization-server"


def get_oauth_endpoints(
    hostname: str, use_azure_auth: bool
) -> Optional[OAuthEndpointCollection]:
    cloud = infer_cloud_from_host(hostname)

    if cloud in [CloudType.AWS, CloudType.GCP]:
        return InHouseOAuthEndpointCollection()
    elif cloud == CloudType.AZURE:
        return (
            InHouseOAuthEndpointCollection()
            if is_supported_databricks_oauth_host(hostname) and not use_azure_auth
            else AzureOAuthEndpointCollection()
        )
    else:
        return None
