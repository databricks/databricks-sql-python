#
# It implements all the cloud specific OAuth configuration/metadata
#
#   Azure: It uses AAD
#   AWS: It uses Databricks internal IdP
#   GCP: Not support yet
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


# Infer cloud type from Databricks SQL instance hostname
def infer_cloud_from_host(hostname: str) -> Optional[CloudType]:
    # normalize
    host = hostname.lower().replace("https://", "").split("/")[0]

    if host.endswith(".azuredatabricks.net"):
        return CloudType.AZURE
    elif host.endswith(".gcp.databricks.com"):
        return CloudType.GCP
    elif host.endswith("cloud.databricks.com"):
        return CloudType.AWS
    else:
        return None


def get_databricks_oidc_url(hostname: str):
    maybe_scheme = "https://" if not hostname.startswith("https://") else ""
    maybe_trailing_slash = "/" if not hostname.endswith("/") else ""
    return f"{maybe_scheme}{hostname}{maybe_trailing_slash}{OIDC_REDIRECTOR_PATH}"


class OAuthEndpoints(ABC):
    @abstractmethod
    def get_scopes_mapping(self, scopes: List[str]) -> List[str]:
        raise NotImplementedError()

    # Endpoint for oauth2 authorization  e.g https://idp.example.com/oauth2/v2.0/authorize
    @abstractmethod
    def get_authorization_endpoint(self, hostname: str) -> str:
        raise NotImplementedError()

    # Endpoint for well-known openid configuration e.g https://idp.example.com/oauth2/.well-known/openid-configuration
    @abstractmethod
    def get_openid_config_endpoint(self, hostname: str) -> str:
        raise NotImplementedError()


class OAuthEndpointsAzure(OAuthEndpoints):
    SCOPE_USER_IMPERSONATION = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/user_impersonation"

    def get_scopes_mapping(self, scopes: List[str]) -> List[str]:
        # There is no corresponding scopes in Azure, instead, access control will be delegated to Databricks
        # To support scope in dev, it can also be set in the environment variable DATABRICKS_AZURE_SCOPE
        azure_scope = (
            os.getenv("DATABRICKS_AZURE_SCOPE")
            or OAuthEndpointsAzure.SCOPE_USER_IMPERSONATION
        )
        ret_scopes = [azure_scope]
        if OAuthScope.OFFLINE_ACCESS in scopes:
            ret_scopes.append(OAuthScope.OFFLINE_ACCESS)
        return ret_scopes

    def get_authorization_endpoint(self, hostname: str):
        # We need get account specific url, which can be redirected by databricks unified oidc endpoint
        return f"{get_databricks_oidc_url(hostname)}/oauth2/v2.0/authorize"

    def get_openid_config_endpoint(self, hostname: str):
        return "https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration"


class OAuthEndpointsAws(OAuthEndpoints):
    def get_scopes_mapping(self, scopes: List[str]) -> List[str]:
        # No scope mapping in AWS
        return scopes

    def get_authorization_endpoint(self, hostname: str):
        idp_url = get_databricks_oidc_url(hostname)
        return f"{idp_url}/oauth2/v2.0/authorize"

    def get_openid_config_endpoint(self, hostname: str):
        idp_url = get_databricks_oidc_url(hostname)
        return f"{idp_url}/.well-known/oauth-authorization-server"


def get_oauth_endpoints(cloud: CloudType) -> Optional[OAuthEndpoints]:
    if cloud == CloudType.AWS:
        return OAuthEndpointsAws()
    elif cloud == CloudType.AZURE:
        return OAuthEndpointsAzure()
    else:
        return None
