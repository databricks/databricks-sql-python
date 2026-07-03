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
def ingestion_user():
    return os.getenv("DATABRICKS_USER")


@pytest.fixture(scope="session")
def catalog():
    return os.getenv("DATABRICKS_CATALOG")


@pytest.fixture(scope="session")
def schema():
    return os.getenv("DATABRICKS_SCHEMA", "default")


@pytest.fixture(scope="session", autouse=True)
def connection_details(host, http_path, access_token, ingestion_user, catalog, schema):
    return {
        "host": host,
        "http_path": http_path,
        "access_token": access_token,
        "ingestion_user": ingestion_user,
        "catalog": catalog,
        "schema": schema,
    }
