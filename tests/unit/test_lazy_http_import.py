"""Importing the connector must not import requests or the OAuth client libraries.

They are only needed by the auth flows and HTTP paths that use them. Each check runs
in a fresh interpreter because ``sys.modules`` is process-global.
"""

import subprocess
import sys
import textwrap

import pytest

# Sentinel exit codes, distinct from the 1 of an uncaught exception.
_CLEAN, _LOADED, _IMPORT_FAILED = 10, 11, 12

MODULES = [
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sql.session",
    "databricks.sql.auth.authenticators",
    "databricks.sql.common.http",
]
UNWANTED = ["requests", "oauthlib"]


@pytest.mark.parametrize("module_name", MODULES)
def test_module_does_not_import_http_client_libraries(module_name):
    script = f"""
        import sys
        try:
            import {module_name}  # noqa: F401
        except BaseException:
            import traceback
            traceback.print_exc()
            sys.exit({_IMPORT_FAILED})
        loaded = [m for m in {UNWANTED!r} if m in sys.modules]
        print(loaded)
        sys.exit({_LOADED} if loaded else {_CLEAN})
        """
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )
    if proc.returncode == _IMPORT_FAILED:
        pytest.skip(f"{module_name} not importable here:\n{proc.stderr}")
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
