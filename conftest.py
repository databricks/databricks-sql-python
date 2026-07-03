import os
import pytest


@pytest.fixture(scope="session")
def host():
    return os.getenv("DATABRICKS_SERVER_HOSTNAME")


@pytest.fixture(scope="session")
def http_path():
    return os.getenv("DATABRICKS_HTTP_PATH")


@pytest.fixture(scope="session")
def access_token():
    return os.getenv("DATABRICKS_TOKEN")


@pytest.fixture(scope="session")
def client_id():
    # OAuth M2M service-principal client id (application id).
    return os.getenv("DATABRICKS_CLIENT_ID")


@pytest.fixture(scope="session")
def client_secret():
    return os.getenv("DATABRICKS_CLIENT_SECRET")


@pytest.fixture(scope="session")
def ingestion_user():
    return os.getenv("DATABRICKS_USER")


@pytest.fixture(scope="session")
def catalog():
    return os.getenv("DATABRICKS_CATALOG")


@pytest.fixture(scope="session")
def schema():
    return os.getenv("DATABRICKS_SCHEMA", "default")


@pytest.fixture(scope="session", autouse=True)
def connection_details(
    host,
    http_path,
    access_token,
    client_id,
    client_secret,
    ingestion_user,
    catalog,
    schema,
):
    return {
        "host": host,
        "http_path": http_path,
        "access_token": access_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "ingestion_user": ingestion_user,
        "catalog": catalog,
        "schema": schema,
    }


def auth_connect_kwargs(details):
    """Return the sql.connect auth kwargs from connection_details.

    Prefers OAuth M2M (service principal) when DATABRICKS_CLIENT_ID /
    DATABRICKS_CLIENT_SECRET are set, otherwise falls back to a PAT
    (DATABRICKS_TOKEN). M2M is required for identity-scoped operations such as
    Personal Staging Location tests (stage://tmp/<DATABRICKS_USER>/...), where
    the connecting identity must equal DATABRICKS_USER -- the service principal.
    A PAT belonging to a different identity cannot access that stage.
    """
    client_id = details.get("client_id")
    client_secret = details.get("client_secret")
    if client_id and client_secret:
        host = details["host"]
        host_url = host if host.startswith("http") else f"https://{host}"

        def credential_provider():
            # Imported lazily so a PAT-only environment doesn't require the SDK.
            from databricks.sdk.core import Config, oauth_service_principal

            return oauth_service_principal(
                Config(
                    host=host_url,
                    client_id=client_id,
                    client_secret=client_secret,
                    # Explicit so an ambient DATABRICKS_TOKEN doesn't collide
                    # ("more than one authorization method configured").
                    auth_type="oauth-m2m",
                )
            )

        return {"credentials_provider": credential_provider}
    return {"access_token": details.get("access_token")}
