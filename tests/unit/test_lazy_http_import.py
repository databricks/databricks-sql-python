"""Importing the connector must not import the Python HTTP and OAuth client stack.

requests, urllib3, oauthlib, jwt and pybreaker are only needed by the auth flows,
the Python HTTP client and telemetry export that use them; a kernel connection uses
none of them. Each check runs in a fresh interpreter because ``sys.modules`` is
process-global.
"""

import subprocess
import sys
import textwrap

import pytest

# Sentinel exit codes, distinct from the 1 of an uncaught exception.
_CLEAN, _LOADED, _MISSING_OPTIONAL_DEP = 10, 11, 12
# Only a missing optional extra may skip; any other import error fails.
_OPTIONAL_DEPS = ("databricks_sql_kernel", "pyarrow")

MODULES = [
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sql.session",
    "databricks.sql.auth.auth",
    "databricks.sql.auth.authenticators",
    "databricks.sql.common.http",
    "databricks.sql.cloudfetch.downloader",
    "databricks.sql.telemetry.telemetry_client",
    "databricks.sql.backend.kernel.client",
]
UNWANTED = ["requests", "urllib3", "oauthlib", "jwt", "pybreaker"]


@pytest.mark.parametrize("module_name", MODULES)
def test_module_does_not_import_http_client_libraries(module_name):
    script = f"""
        import sys
        try:
            import {module_name}  # noqa: F401
        except ImportError as e:
            # The kernel backend re-raises a missing wheel as a friendlier
            # ImportError, so look for the missing extra along the chain.
            cause = e
            while cause is not None and not (
                isinstance(cause, ModuleNotFoundError)
                and cause.name in {_OPTIONAL_DEPS!r}
            ):
                cause = cause.__cause__ or cause.__context__
            if cause is None:
                raise
            print(cause.name)
            sys.exit({_MISSING_OPTIONAL_DEP})
        loaded = [m for m in {UNWANTED!r} if m in sys.modules]
        print(loaded)
        sys.exit({_LOADED} if loaded else {_CLEAN})
        """
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )
    if proc.returncode == _MISSING_OPTIONAL_DEP:
        pytest.skip(f"{module_name} needs {proc.stdout.strip()}, not installed")
    assert proc.returncode != _LOADED, f"importing {module_name} loaded {proc.stdout}"
    assert (
        proc.returncode == _CLEAN
    ), f"unexpected exit {proc.returncode}:\n{proc.stderr}"


def test_oauth_provider_still_builds_its_manager():
    from unittest.mock import MagicMock, patch

    from databricks.sql.auth.authenticators import DatabricksOAuthProvider

    with patch("databricks.sql.auth.oauth.OAuthManager") as manager, patch.object(
        DatabricksOAuthProvider, "_initial_get_token"
    ):
        DatabricksOAuthProvider(
            hostname="example.cloud.databricks.com",
            oauth_persistence=None,
            redirect_port_range=[8020],
            client_id="client",
            scopes=["sql"],
            http_client=MagicMock(),
        )
    manager.assert_called_once()


def test_token_federation_check_does_not_import_it():
    script = """
        import sys
        from databricks.sql.auth.common import is_token_federation_provider
        from databricks.sql.auth.authenticators import AccessTokenAuthProvider
        assert not is_token_federation_provider(AccessTokenAuthProvider("t"))
        assert "databricks.sql.auth.token_federation" not in sys.modules
        """
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr


def test_token_federation_check_matches_real_providers():
    from unittest.mock import MagicMock

    from databricks.sql.auth.authenticators import AccessTokenAuthProvider
    from databricks.sql.auth.common import is_token_federation_provider
    from databricks.sql.auth.token_federation import TokenFederationProvider
    from databricks.sql.telemetry.models.enums import AuthMech
    from databricks.sql.telemetry.telemetry_client import TelemetryHelper

    wrapped = TokenFederationProvider(
        hostname="example.cloud.databricks.com",
        external_provider=AccessTokenAuthProvider("t"),
        http_client=MagicMock(),
    )
    assert is_token_federation_provider(wrapped)
    assert TelemetryHelper.get_auth_mechanism(wrapped) == AuthMech.PAT
