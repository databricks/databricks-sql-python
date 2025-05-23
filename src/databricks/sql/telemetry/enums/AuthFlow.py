from enum import Enum


class AuthFlow(Enum):
    TOKEN_PASSTHROUGH = "token_passthrough"
    CLIENT_CREDENTIALS = "client_credentials"
    BROWSER_BASED_AUTHENTICATION = "browser_based_authentication"
    AZURE_MANAGED_IDENTITIES = "azure_managed_identities"
