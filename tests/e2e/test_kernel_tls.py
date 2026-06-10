"""E2E TLS tests for ``use_kernel=True``, exercised through an
intercepting HTTPS proxy (mitmproxy) against a live Databricks
workspace.

This is the connector-level counterpart to the kernel repo's
``v0_tls_e2e.rs``. The kernel test proves the raw TLS handshake against
the kernel's ``TlsConfig`` directly; this one proves the *whole stack*:
``sql.connect(use_kernel=True, _tls_*=...)`` → ``SSLOptions`` →
``_kernel_tls_kwargs`` → pyo3 → kernel handshake.

mitmproxy sits in front of the workspace and re-signs every TLS
connection with its own CA. A client trusting only the system roots
sees an untrusted cert and fails — unless we add mitmproxy's CA via
``_tls_trusted_ca_file`` or disable validation via ``_tls_no_verify``.
The three tests cover exactly those outcomes (reject / CA-trusted /
no-verify).

Traffic is routed through mitmproxy via the ``HTTPS_PROXY`` env var
(reqwest honours it by default), since the connector's proxy surface
isn't yet wired to the kernel. The CI job sets
``HTTPS_PROXY=http://localhost:8080``.

Skipped automatically when the kernel wheel, the live creds, or
``MITMPROXY_CA_CERT`` are absent — so it's a no-op in the default test
run and only fires in the ``kernel-e2e`` TLS CI job (or locally with
mitmproxy up; see that workflow / the kernel repo's ``v0_tls_e2e.rs``
header for the docker incantation).
"""

from __future__ import annotations

import os
import sys

import pytest

import databricks.sql as sql
from databricks.sql.exc import Error as DatabricksSqlError

# Same real-wheel guard as test_kernel_backend.py — see the detailed
# rationale there. Skip only when the wheel is genuinely not installed;
# FAIL LOUDLY if it's installed but shadowed by a stub (so a misconfigured
# shared pytest session can't silently pass as covering the kernel).
import importlib.metadata as _ilm

try:
    _ilm.version("databricks-sql-kernel")
    _kernel_installed = True
except _ilm.PackageNotFoundError:
    _kernel_installed = False

_kernel_mod = sys.modules.get("databricks_sql_kernel")
if _kernel_mod is None:
    try:
        import databricks_sql_kernel as _kernel_mod  # type: ignore[import-not-found]
    except ImportError:
        _kernel_mod = None
_kernel_is_real = _kernel_mod is not None and getattr(_kernel_mod, "__file__", None)

if not _kernel_installed:
    pytest.skip(
        "databricks-sql-kernel is not installed; install the real wheel "
        "to run kernel TLS e2e tests",
        allow_module_level=True,
    )
elif not _kernel_is_real:
    raise RuntimeError(
        "databricks-sql-kernel is installed but sys.modules holds a stub "
        "(no __file__) — the kernel TLS e2e tests would not exercise the "
        "real wheel. Run them in isolation (separate pytest invocation) so "
        "a unit-test fake module doesn't shadow the real one."
    )

_MITM_CA = os.getenv("MITMPROXY_CA_CERT")
if not _MITM_CA:
    pytest.skip(
        "MITMPROXY_CA_CERT not set — TLS e2e runs only behind the "
        "mitmproxy CI job (or a local mitmproxy)",
        allow_module_level=True,
    )


def _env(key):
    v = os.getenv(key)
    return v if v else None


@pytest.fixture(scope="module")
def base_params(connection_details):
    """Live workspace connection params for use_kernel=True. Prefers
    OAuth M2M (so the TLS suite doubles as M2M-through-proxy coverage)
    and falls back to PAT."""
    host = connection_details.get("host")
    http_path = connection_details.get("http_path")
    if not (host and http_path):
        pytest.skip("DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH not set")

    params = {
        "server_hostname": host,
        "http_path": http_path,
        "use_kernel": True,
    }

    client_id = _env("DATABRICKS_TEST_CLIENT_ID")
    client_secret = _env("DATABRICKS_TEST_CLIENT_SECRET")
    token = connection_details.get("access_token")
    if client_id and client_secret:
        params["oauth_client_id"] = client_id
        params["oauth_client_secret"] = client_secret
    elif token:
        params["access_token"] = token
    else:
        pytest.skip(
            "need DATABRICKS_TEST_CLIENT_ID/SECRET (OAuth M2M) or "
            "DATABRICKS_TOKEN (PAT)"
        )
    return params


def _select_one(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS n")
        rows = cur.fetchall()
    assert len(rows) == 1 and rows[0][0] == 1


def test_tls_fails_without_config_behind_intercepting_proxy(base_params):
    """Strict default (no CA, verify on) must reject mitmproxy's
    re-signed certificate. Guards that the positive tests below are
    actually validating the chain rather than bypassing the proxy."""
    with pytest.raises(DatabricksSqlError):
        conn = sql.connect(**base_params)
        try:
            # Some auth flows defer the first network call until a query.
            _select_one(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass


def test_tls_with_trusted_custom_ca_succeeds(base_params):
    """Adding mitmproxy's CA via ``_tls_trusted_ca_file`` makes the
    re-signed cert trusted, so the full round-trip succeeds. Proves
    ``_tls_trusted_ca_file`` is wired through to the kernel handshake."""
    conn = sql.connect(_tls_trusted_ca_file=_MITM_CA, **base_params)
    try:
        _select_one(conn)
    finally:
        conn.close()


def test_tls_with_no_verify_succeeds(base_params):
    """``_tls_no_verify=True`` disables chain validation, so the
    round-trip succeeds without supplying the CA."""
    conn = sql.connect(_tls_no_verify=True, **base_params)
    try:
        _select_one(conn)
    finally:
        conn.close()
