from enum import Enum
from typing import Optional


class AuthType(Enum):
    DATABRICKS_OAUTH = "databricks-oauth"
    AZURE_OAUTH = "azure-oauth"
    AZURE_SP_M2M = "azure-sp-m2m"


def get_effective_azure_login_app_id(hostname) -> str:
    """
    Get the effective Azure login app ID for a given hostname.
    This function determines the appropriate Azure login app ID based on the hostname.
    If the hostname does not match any of these domains, it returns the default Databricks resource ID.

    """
    azure_app_ids = {
        ".dev.azuredatabricks.net": "62a912ac-b58e-4c1d-89ea-b2dbfc7358fc",
        ".staging.azuredatabricks.net": "4a67d088-db5c-48f1-9ff2-0aace800ae68",
    }

    for domain, app_id in azure_app_ids.items():
        if domain in hostname:
            return app_id

    # default databricks resource id
    return "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
