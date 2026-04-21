"""
Full SPOG E2E test suite for Databricks SQL Python Connector.

Tests all auth flows on BOTH SPOG and legacy Azure workspaces:
  1. PAT
  2. Databricks M2M (Databricks managed SP via workspace OIDC)
  3. Entra ID M2M (Azure AD app via login.microsoftonline.com, auth_type=azure-sp-m2m)
  4. U2M (browser OAuth - skipped by default)
  5. Telemetry endpoint
  6. Feature flags endpoint

SPOG workspace:
  Host: peco.azuredatabricks.net
  Warehouse: /sql/1.0/warehouses/00adc7b6c00429b8?o=6436897454825492
  Workspace ID: 6436897454825492

Legacy workspace:
  Host: adb-6436897454825492.12.azuredatabricks.net
  Warehouse: /sql/1.0/warehouses/00adc7b6c00429b8

Env vars:
  DATABRICKS_PECOTESTING_TOKEN                           -- PAT (works on both SPOG and legacy)
  DATABRICKS_PECO_CLIENT_ID                              -- Databricks M2M client ID
  DATABRICKS_PECO_CLIENT_SECRET                          -- Databricks M2M client secret
  DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID                   -- Entra/Azure AD app client ID
  DATABRICKS_SPOG_ENTRA_TEST_CLIENT_SECRET               -- Entra/Azure AD app client secret

Usage:
    cd /path/to/databricks-sql-python
    PYTHONPATH=src .venv/bin/python examples/spog_peco_test.py
"""

import json
import os
import sys
import time
import urllib3

from databricks import sql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPOG_HOST = "peco.azuredatabricks.net"
LEGACY_HOST = "adb-6436897454825492.12.azuredatabricks.net"
WORKSPACE_ID = "6436897454825492"

SPOG_HTTP_PATH = f"/sql/1.0/warehouses/00adc7b6c00429b8?o={WORKSPACE_ID}"
LEGACY_HTTP_PATH = "/sql/1.0/warehouses/00adc7b6c00429b8"

AZURE_TENANT_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"

FF_DRIVER = "PYTHON"
FF_VERSION = "4.2.5"

# Credentials from env
# Try personal PAT first, fall back to regular
PAT = os.getenv("DATABRICKS_PECOTESTING_TOKEN_PERSONAL") or os.getenv("DATABRICKS_PECOTESTING_TOKEN")

# Databricks managed SP for this workspace
M2M_CLIENT_ID = os.getenv("DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_ID_MSR_SPN")
M2M_CLIENT_SECRET = os.getenv("DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_SECRET_MSR_SPN")

# Azure AD (Entra) app registered in this workspace's tenant
ENTRA_CLIENT_ID = os.getenv("DATABRICKS_PECOTESING_AAD_CLIENT_ID")
ENTRA_CLIENT_SECRET = os.getenv("DATABRICKS_PECOTESING_AAD_CLIENT_SECRET")

U2M_ENABLED = os.getenv("DATABRICKS_SPOG_U2M_ENABLED") == "true"

# Counters
passed = 0
failed = 0
skipped = 0
results = []

# Disable urllib3 SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------
# Workspace-level OIDC credentials provider (Databricks M2M)
# ---------------------------------------------------------------------------

def _workspace_oidc_token(host, client_id, client_secret):
    """
    Fetch token from workspace-level OIDC endpoint.
    POST https://{host}/oidc/v1/token with client_credentials grant.
    """
    url = f"https://{host}/oidc/v1/token"
    http = urllib3.PoolManager()
    resp = http.request(
        "POST",
        url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body=(
            f"grant_type=client_credentials"
            f"&client_id={client_id}"
            f"&client_secret={client_secret}"
            f"&scope=all-apis"
        ),
    )
    if resp.status != 200:
        raise RuntimeError(
            f"OIDC token request to {url} failed: HTTP {resp.status}: "
            f"{resp.data.decode('utf-8', errors='replace')[:300]}"
        )
    data = json.loads(resp.data.decode("utf-8"))
    return data["access_token"]


def make_databricks_m2m_provider(host, client_id, client_secret):
    """Build a credentials_provider callable for Databricks M2M."""
    def credentials_provider():
        token = _workspace_oidc_token(host, client_id, client_secret)
        def header_factory():
            return {"Authorization": f"Bearer {token}"}
        return header_factory
    return credentials_provider


# ---------------------------------------------------------------------------
# Test runner helpers
# ---------------------------------------------------------------------------

COL_WIDTH = 60


def _record(name, status, elapsed, error=None):
    global passed, failed, skipped
    if status == "PASS":
        passed += 1
    elif status == "FAIL":
        failed += 1
    elif status == "SKIP":
        skipped += 1
    results.append((name, status, elapsed, error))


def run_test(name, host, http_path, token=None, credentials_provider=None,
             auth_type=None, extra_kwargs=None):
    """Connect, run SELECT 1, verify result == 1, close."""
    print(f"  {name:<{COL_WIDTH}}", end="", flush=True)
    t0 = time.time()
    try:
        kwargs = {
            "server_hostname": host,
            "http_path": http_path,
        }
        if token:
            kwargs["access_token"] = token
        if credentials_provider:
            kwargs["credentials_provider"] = credentials_provider
        if auth_type:
            kwargs["auth_type"] = auth_type
        if extra_kwargs:
            kwargs.update(extra_kwargs)

        conn = sql.connect(**kwargs)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS v")
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        elapsed = time.time() - t0

        if row is None:
            print(f"FAIL  ({elapsed:.1f}s)")
            print(f"       -> fetchone() returned None")
            _record(name, "FAIL", elapsed, "fetchone() returned None")
        elif row[0] != 1:
            print(f"FAIL  ({elapsed:.1f}s)")
            print(f"       -> Expected 1, got {row[0]!r}")
            _record(name, "FAIL", elapsed, f"Expected 1, got {row[0]!r}")
        else:
            print(f"PASS  ({elapsed:.1f}s)")
            _record(name, "PASS", elapsed)

    except Exception as e:
        elapsed = time.time() - t0
        err_msg = f"{type(e).__name__}: {str(e)[:300]}"
        print(f"FAIL  ({elapsed:.1f}s)")
        print(f"       -> {err_msg}")
        _record(name, "FAIL", elapsed, err_msg)


def skip_test(name, reason):
    print(f"  {name:<{COL_WIDTH}}SKIP  ({reason})")
    _record(name, "SKIP", 0.0, reason)


def section(title):
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


# ---------------------------------------------------------------------------
# HTTP endpoint tests
# ---------------------------------------------------------------------------

def test_telemetry(host, label_prefix=""):
    """Direct HTTP POST to /telemetry-ext, verify HTTP 200."""
    name = f"{label_prefix}Telemetry | POST /telemetry-ext"
    print(f"  {name:<{COL_WIDTH}}", end="", flush=True)

    if not PAT:
        print("SKIP  (no PAT)")
        _record(name, "SKIP", 0.0, "no PAT")
        return

    t0 = time.time()
    try:
        http = urllib3.PoolManager()
        resp = http.request(
            "POST",
            f"https://{host}/telemetry-ext",
            headers={
                "Authorization": f"Bearer {PAT}",
                "Content-Type": "application/json",
            },
            body=json.dumps({"items": [], "protoLogs": []}).encode("utf-8"),
            timeout=30.0,
        )
        elapsed = time.time() - t0
        if resp.status == 200:
            print(f"PASS  ({elapsed:.1f}s)")
            _record(name, "PASS", elapsed)
        else:
            body = resp.data.decode("utf-8", errors="replace")[:200]
            print(f"FAIL  ({elapsed:.1f}s)")
            print(f"       -> HTTP {resp.status}: {body}")
            _record(name, "FAIL", elapsed, f"HTTP {resp.status}: {body}")
    except Exception as e:
        elapsed = time.time() - t0
        err_msg = f"{type(e).__name__}: {str(e)[:200]}"
        print(f"FAIL  ({elapsed:.1f}s)")
        print(f"       -> {err_msg}")
        _record(name, "FAIL", elapsed, err_msg)


def test_feature_flags(host, label_prefix=""):
    """Direct HTTP GET to feature flags endpoint."""
    path = f"/api/2.0/connector-service/feature-flags/{FF_DRIVER}/{FF_VERSION}"
    name = f"{label_prefix}Feature Flags | GET {path}"
    print(f"  {name:<{COL_WIDTH}}", end="", flush=True)

    if not PAT:
        print("SKIP  (no PAT)")
        _record(name, "SKIP", 0.0, "no PAT")
        return

    t0 = time.time()
    try:
        http = urllib3.PoolManager()
        resp = http.request(
            "GET",
            f"https://{host}{path}",
            headers={"Authorization": f"Bearer {PAT}"},
            timeout=30.0,
        )
        elapsed = time.time() - t0
        if resp.status == 200:
            print(f"PASS  ({elapsed:.1f}s)")
            _record(name, "PASS", elapsed)
        else:
            body = resp.data.decode("utf-8", errors="replace")[:200]
            print(f"FAIL  ({elapsed:.1f}s)")
            print(f"       -> HTTP {resp.status}: {body}")
            _record(name, "FAIL", elapsed, f"HTTP {resp.status}: {body}")
    except Exception as e:
        elapsed = time.time() - t0
        err_msg = f"{type(e).__name__}: {str(e)[:200]}"
        print(f"FAIL  ({elapsed:.1f}s)")
        print(f"       -> {err_msg}")
        _record(name, "FAIL", elapsed, err_msg)


# ---------------------------------------------------------------------------
# Test groups
# ---------------------------------------------------------------------------

def test_pat(host, http_path, label_prefix=""):
    """PAT auth."""
    if not PAT:
        skip_test(f"{label_prefix}PAT", "DATABRICKS_PECOTESTING_TOKEN not set")
        return
    run_test(f"{label_prefix}PAT", host, http_path, token=PAT)


def test_databricks_m2m(host, http_path, label_prefix=""):
    """Databricks M2M (workspace OIDC)."""
    if not (M2M_CLIENT_ID and M2M_CLIENT_SECRET):
        skip_test(f"{label_prefix}Databricks M2M", "M2M creds not set")
        return

    # For SPOG, the OIDC endpoint is on the legacy host (SPOG OIDC returns account-level tokens)
    # We need to get the token from the legacy host and use it on SPOG
    # Actually, let's try the M2M directly on the target host to see what happens
    provider = make_databricks_m2m_provider(host, M2M_CLIENT_ID, M2M_CLIENT_SECRET)
    run_test(f"{label_prefix}Databricks M2M (workspace OIDC)", host, http_path,
             credentials_provider=provider)


def test_entra_m2m(host, http_path, label_prefix=""):
    """Entra M2M (Azure AD SP via auth_type=azure-sp-m2m)."""
    if not (ENTRA_CLIENT_ID and ENTRA_CLIENT_SECRET):
        skip_test(f"{label_prefix}Entra M2M (azure-sp-m2m)", "Entra creds not set")
        return

    # Test with explicit tenant ID
    run_test(
        f"{label_prefix}Entra M2M (explicit tenant)",
        host, http_path,
        auth_type="azure-sp-m2m",
        extra_kwargs={
            "azure_client_id": ENTRA_CLIENT_ID,
            "azure_client_secret": ENTRA_CLIENT_SECRET,
            "azure_tenant_id": AZURE_TENANT_ID,
        },
    )

    # Test with auto-discovered tenant (via /aad/auth)
    run_test(
        f"{label_prefix}Entra M2M (auto-discover tenant)",
        host, http_path,
        auth_type="azure-sp-m2m",
        extra_kwargs={
            "azure_client_id": ENTRA_CLIENT_ID,
            "azure_client_secret": ENTRA_CLIENT_SECRET,
            # azure_tenant_id omitted → connector calls /aad/auth to discover
        },
    )


def test_u2m(host, http_path, label_prefix=""):
    """U2M (browser OAuth)."""
    if not U2M_ENABLED:
        skip_test(f"{label_prefix}U2M (browser)", "DATABRICKS_SPOG_U2M_ENABLED != true")
        return
    run_test(f"{label_prefix}U2M (browser)", host, http_path,
             auth_type="databricks-oauth")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary():
    print(f"\n{'=' * 80}")
    print("  SUMMARY")
    print(f"{'=' * 80}")
    print(f"  {'Test':<{COL_WIDTH}}{'Result':<8}{'Time':>6}")
    print(f"  {'-' * COL_WIDTH}{'-' * 8}{'-' * 6}")
    for name, status, elapsed, error in results:
        color = ""
        reset = ""
        if sys.stdout.isatty():
            if status == "PASS":
                color = "\033[32m"
            elif status == "FAIL":
                color = "\033[31m"
            elif status == "SKIP":
                color = "\033[33m"
            reset = "\033[0m"
        time_str = f"{elapsed:.1f}s" if elapsed > 0 else "-"
        print(f"  {name:<{COL_WIDTH}}{color}{status:<8}{reset}{time_str:>6}")
    print()
    total = passed + failed + skipped
    print(f"  Total: {total}  |  Passed: {passed}  |  Failed: {failed}  |  Skipped: {skipped}")

    if failed > 0:
        print(f"\n  FAILED TESTS:")
        for name, status, elapsed, error in results:
            if status == "FAIL":
                print(f"    - {name}")
                if error:
                    print(f"      {error}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print()
    print("=" * 80)
    print("  Databricks SQL Python Connector — SPOG Peco E2E Test Suite")
    print("=" * 80)

    # Show detected credentials
    print(f"\n  Credentials detected:")
    print(f"    PAT .......................... {'YES' if PAT else 'NO'}")
    print(f"    Databricks M2M .............. {'YES' if (M2M_CLIENT_ID and M2M_CLIENT_SECRET) else 'NO'}")
    print(f"    Entra ID M2M ................ {'YES' if (ENTRA_CLIENT_ID and ENTRA_CLIENT_SECRET) else 'NO'}")
    print(f"    U2M (browser) ............... {'YES' if U2M_ENABLED else 'NO'}")

    # =====================================================================
    # LEGACY workspace tests
    # =====================================================================
    section(f"LEGACY — {LEGACY_HOST}")
    print(f"  HTTP path: {LEGACY_HTTP_PATH}\n")

    test_pat(LEGACY_HOST, LEGACY_HTTP_PATH, "Legacy | ")
    test_databricks_m2m(LEGACY_HOST, LEGACY_HTTP_PATH, "Legacy | ")
    test_entra_m2m(LEGACY_HOST, LEGACY_HTTP_PATH, "Legacy | ")
    test_u2m(LEGACY_HOST, LEGACY_HTTP_PATH, "Legacy | ")

    # =====================================================================
    # SPOG workspace tests
    # =====================================================================
    section(f"SPOG — {SPOG_HOST}")
    print(f"  HTTP path: {SPOG_HTTP_PATH}\n")

    test_pat(SPOG_HOST, SPOG_HTTP_PATH, "SPOG | ")
    test_databricks_m2m(SPOG_HOST, SPOG_HTTP_PATH, "SPOG | ")
    test_entra_m2m(SPOG_HOST, SPOG_HTTP_PATH, "SPOG | ")
    test_u2m(SPOG_HOST, SPOG_HTTP_PATH, "SPOG | ")

    # =====================================================================
    # Endpoint tests (SPOG only — these are the interesting ones)
    # =====================================================================
    section("Endpoints (SPOG)")
    test_telemetry(SPOG_HOST, "SPOG | ")
    test_feature_flags(SPOG_HOST, "SPOG | ")

    section("Endpoints (Legacy)")
    test_telemetry(LEGACY_HOST, "Legacy | ")
    test_feature_flags(LEGACY_HOST, "Legacy | ")

    # Print summary
    print_summary()

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
