import importlib.util
import os
import pytest


def _kernel_wheel_available() -> bool:
    """The ``use_sea=True`` code path now routes through the Rust
    kernel via PyO3. The ``databricks_sql_kernel`` wheel is not
    yet on PyPI (built from a separate repo); CI environments
    without it should skip ``use_sea=True`` parametrized cases
    rather than fail with a hard ImportError."""
    return importlib.util.find_spec("databricks_sql_kernel") is not None


def pytest_collection_modifyitems(config, items):
    """Skip parametrized test cases that pass ``use_sea=True`` when
    the kernel wheel isn't installed.

    The existing e2e suite uses ``@pytest.mark.parametrize(
    "extra_params", [{}, {"use_sea": True}])`` to exercise both
    backends. When the kernel wheel is missing those cases die at
    ``connect()`` time with our pointed ImportError; mark them
    skipped at collection time so CI signal stays accurate.
    """
    if _kernel_wheel_available():
        return
    skip_marker = pytest.mark.skip(
        reason="use_sea=True requires databricks-sql-kernel (not installed)"
    )
    for item in items:
        params = getattr(item, "callspec", None)
        if params is None:
            continue
        extra_params = params.params.get("extra_params")
        if isinstance(extra_params, dict) and extra_params.get("use_sea") is True:
            item.add_marker(skip_marker)


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
