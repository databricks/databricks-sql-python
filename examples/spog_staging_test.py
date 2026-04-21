"""
Python SQL Connector — SPOG Staging E2E Test

Tests all auth flows on the staging SPOG workspace.

Auth flows tested:
  1. PAT (access_token)
  2. SDK M2M (credentials_provider via databricks-sdk oauth_service_principal)
     → sends to {host}/oidc/v1/token (Databricks OIDC)
  3. Azure SP M2M (auth_type="azure-sp-m2m" with explicit tenant)
     → sends to login.microsoftonline.com/{tenant}/oauth2/token (Azure AD)
  4. Azure SP M2M with auto-discovered tenant (no azure_tenant_id)
     → calls /aad/auth to discover tenant, then sends to Azure AD

Workspaces:
  SPOG:   dogfood-spog.staging.azuredatabricks.net
  Legacy: adb-7064161269814046.2.staging.azuredatabricks.net
  Warehouse: /sql/1.0/warehouses/e256699345d1ac74
  SPOG path: /sql/1.0/warehouses/e256699345d1ac74?o=7064161269814046
  Tenant: e3fe3f22-4b98-4c04-82cc-d8817d1b17da

Env vars (from JDBC test):
  DATABRICKS_DOGFOOD_WESTUS_STAGING_TOKEN           — PAT
  DATABRICKS_DOGFOOD_AZURE_CLIENT_ID/SECRET         — Databricks managed SP (dose secret)
  DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID/SECRET       — Azure AD app (Azure Portal secret)
  DATABRICKS_AAD_CLIENT_ID/SECRET                   — Azure AD app (Azure Portal secret)

Usage:
    source ~/.zshrc
    cd databricks-sql-python
    PYTHONPATH=src .venv/bin/python examples/spog_staging_test.py
"""

import os
import sys
import time
import traceback

# Must be first — so credentials_provider import works
from databricks import sql
from databricks.sdk.core import oauth_service_principal, Config

# ---------------------------------------------------------------------------
# Configuration — matches OSS JDBC OssJdbcSpogFullTest.java
# ---------------------------------------------------------------------------

SPOG_HOST = "dogfood-spog.staging.azuredatabricks.net"
LEGACY_HOST = "adb-7064161269814046.2.staging.azuredatabricks.net"
WAREHOUSE = "/sql/1.0/warehouses/e256699345d1ac74"
SPOG_PATH = WAREHOUSE + "?o=7064161269814046"
TENANT = "e3fe3f22-4b98-4c04-82cc-d8817d1b17da"

# ---------------------------------------------------------------------------
# Credentials from env
# ---------------------------------------------------------------------------

PAT = os.getenv("DATABRICKS_DOGFOOD_WESTUS_STAGING_TOKEN")

# SDK M2M: Databricks managed SP with Databricks secret (dose...)
# Same as JDBC test: DATABRICKS_DOGFOOD_AZURE_CLIENT_ID/SECRET
SDK_M2M_ID = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")
SDK_M2M_SECRET = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")

# Azure AD app with Azure Portal secret (eAA8...)
# Same as JDBC test: DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID/SECRET
ENTRA_ID = os.getenv("DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID")
ENTRA_SECRET = os.getenv("DATABRICKS_SPOG_ENTRA_TEST_CLIENT_SECRET")

# Another Azure AD app with Azure Portal secret (US28...)
AAD_ID = os.getenv("DATABRICKS_AAD_CLIENT_ID")
AAD_SECRET = os.getenv("DATABRICKS_AAD_CLIENT_SECRET")

# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

results = []
COL = 62


def record(name, status, elapsed=0, error=None):
    results.append((name, status, elapsed, error))
    tag = {"PASS": "\033[32mPASS\033[0m", "FAIL": "\033[31mFAIL\033[0m", "SKIP": "\033[33mSKIP\033[0m"}.get(status, status)
    time_str = f"({elapsed:.1f}s)" if elapsed > 0 else ""
    print(f"  {name:<{COL}}{tag}  {time_str}")
    if error:
        # Truncate long errors
        err = str(error)[:300]
        print(f"    -> {err}")


def run_query_test(name, **connect_kwargs):
    """Connect, SELECT 1, verify, close."""
    t0 = time.time()
    try:
        conn = sql.connect(**connect_kwargs)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS v")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        elapsed = time.time() - t0
        if row and row[0] == 1:
            record(name, "PASS", elapsed)
        else:
            record(name, "FAIL", elapsed, f"Expected 1, got {row}")
    except Exception as e:
        elapsed = time.time() - t0
        record(name, "FAIL", elapsed, f"{type(e).__name__}: {e}")


def skip(name, reason):
    record(name, "SKIP", error=reason)


def section(title):
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


# ---------------------------------------------------------------------------
# SDK M2M credentials_provider builder
# Exactly like examples/m2m_oauth.py
# ---------------------------------------------------------------------------

def make_sdk_m2m_provider(hostname, client_id, client_secret):
    """Build credentials_provider using databricks-sdk oauth_service_principal."""
    def credential_provider():
        config = Config(
            host=f"https://{hostname}",
            client_id=client_id,
            client_secret=client_secret,
        )
        return oauth_service_principal(config)
    return credential_provider


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_pat():
    section("1. PAT Authentication")
    if not PAT:
        skip("PAT | SPOG", "DATABRICKS_DOGFOOD_WESTUS_STAGING_TOKEN not set")
        skip("PAT | Legacy", "")
        return
    run_query_test("PAT | SPOG",
                   server_hostname=SPOG_HOST, http_path=SPOG_PATH, access_token=PAT)
    run_query_test("PAT | Legacy",
                   server_hostname=LEGACY_HOST, http_path=WAREHOUSE, access_token=PAT)


def test_sdk_m2m():
    section("2. SDK M2M (Databricks OIDC via oauth_service_principal)")
    if not (SDK_M2M_ID and SDK_M2M_SECRET):
        skip("SDK M2M | SPOG", "DATABRICKS_DOGFOOD_AZURE_CLIENT_ID/SECRET not set")
        skip("SDK M2M | Legacy", "")
        return

    provider = make_sdk_m2m_provider(SPOG_HOST, SDK_M2M_ID, SDK_M2M_SECRET)
    run_query_test("SDK M2M (dc8dd813) | SPOG",
                   server_hostname=SPOG_HOST, http_path=SPOG_PATH,
                   credentials_provider=provider)

    provider_legacy = make_sdk_m2m_provider(LEGACY_HOST, SDK_M2M_ID, SDK_M2M_SECRET)
    run_query_test("SDK M2M (dc8dd813) | Legacy",
                   server_hostname=LEGACY_HOST, http_path=WAREHOUSE,
                   credentials_provider=provider_legacy)


def test_azure_sp_m2m():
    section("3. Azure SP M2M (login.microsoftonline.com via auth_type=azure-sp-m2m)")

    # 3a. SPOG_ENTRA_TEST — Azure AD app (d7f11108) with Azure secret (eAA8...)
    if ENTRA_ID and ENTRA_SECRET:
        run_query_test(
            "Azure SP M2M (d7f11108, explicit tenant) | SPOG",
            server_hostname=SPOG_HOST, http_path=SPOG_PATH,
            auth_type="azure-sp-m2m",
            azure_client_id=ENTRA_ID,
            azure_client_secret=ENTRA_SECRET,
            azure_tenant_id=TENANT,
        )
        run_query_test(
            "Azure SP M2M (d7f11108, explicit tenant) | Legacy",
            server_hostname=LEGACY_HOST, http_path=WAREHOUSE,
            auth_type="azure-sp-m2m",
            azure_client_id=ENTRA_ID,
            azure_client_secret=ENTRA_SECRET,
            azure_tenant_id=TENANT,
        )
    else:
        skip("Azure SP M2M (d7f11108) | SPOG", "DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID/SECRET not set")
        skip("Azure SP M2M (d7f11108) | Legacy", "")

    # 3b. AAD_CLIENT — Azure AD app (d154b9ed) with Azure secret (US28...)
    if AAD_ID and AAD_SECRET:
        run_query_test(
            "Azure SP M2M (d154b9ed, explicit tenant) | SPOG",
            server_hostname=SPOG_HOST, http_path=SPOG_PATH,
            auth_type="azure-sp-m2m",
            azure_client_id=AAD_ID,
            azure_client_secret=AAD_SECRET,
            azure_tenant_id=TENANT,
        )
        run_query_test(
            "Azure SP M2M (d154b9ed, explicit tenant) | Legacy",
            server_hostname=LEGACY_HOST, http_path=WAREHOUSE,
            auth_type="azure-sp-m2m",
            azure_client_id=AAD_ID,
            azure_client_secret=AAD_SECRET,
            azure_tenant_id=TENANT,
        )
    else:
        skip("Azure SP M2M (d154b9ed) | SPOG", "DATABRICKS_AAD_CLIENT_ID/SECRET not set")
        skip("Azure SP M2M (d154b9ed) | Legacy", "")


def test_azure_sp_m2m_auto_tenant():
    section("4. Azure SP M2M — auto-discover tenant via /aad/auth")
    # On staging, /aad/auth returns 403, so this is expected to fail
    if ENTRA_ID and ENTRA_SECRET:
        run_query_test(
            "Azure SP M2M (d7f11108, auto-tenant) | Legacy",
            server_hostname=LEGACY_HOST, http_path=WAREHOUSE,
            auth_type="azure-sp-m2m",
            azure_client_id=ENTRA_ID,
            azure_client_secret=ENTRA_SECRET,
            # azure_tenant_id omitted — connector calls /aad/auth
        )
    else:
        skip("Azure SP M2M (auto-tenant) | Legacy", "no Entra creds")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print()
    print("=" * 80)
    print("  Python SQL Connector — SPOG Staging E2E Test")
    print(f"  SPOG:   {SPOG_HOST}")
    print(f"  Legacy: {LEGACY_HOST}")
    print(f"  Warehouse: {WAREHOUSE}")
    print("=" * 80)
    print()
    print("  Credentials:")
    print(f"    PAT ........................ {'YES' if PAT else 'NO'}")
    print(f"    SDK M2M (dc8dd813) ......... {'YES' if SDK_M2M_ID else 'NO'}  (dose secret → Databricks OIDC)")
    print(f"    Entra (d7f11108) ........... {'YES' if ENTRA_ID else 'NO'}  (Azure secret → login.microsoftonline.com)")
    print(f"    AAD (d154b9ed) ............. {'YES' if AAD_ID else 'NO'}  (Azure secret → login.microsoftonline.com)")

    test_pat()
    test_sdk_m2m()
    test_azure_sp_m2m()
    test_azure_sp_m2m_auto_tenant()

    # Summary
    print(f"\n{'=' * 80}")
    print("  SUMMARY")
    print(f"{'=' * 80}")
    p = sum(1 for _, s, _, _ in results if s == "PASS")
    f = sum(1 for _, s, _, _ in results if s == "FAIL")
    s = sum(1 for _, s, _, _ in results if s == "SKIP")
    print(f"  Total: {len(results)}  |  PASS: {p}  |  FAIL: {f}  |  SKIP: {s}")
    if f:
        print(f"\n  FAILURES:")
        for name, status, _, err in results:
            if status == "FAIL":
                print(f"    - {name}")
                if err:
                    print(f"      {str(err)[:200]}")
    print()
    sys.exit(1 if f > 0 else 0)


if __name__ == "__main__":
    main()
