"""
Python SQL Connector — Complete Auth Flow Test (M2M + U2M)

Tests every auth path the Python connector supports, on both SPOG and legacy workspaces.

M2M Paths:
  1. SDK M2M via credentials_provider (documented path)
     → uses databricks-sdk oauth_service_principal
     → sends to {host}/oidc/v1/token (Databricks OIDC)
     → wrapped by TokenFederationProvider

  2. auth_type="azure-sp-m2m" with explicit azure_tenant_id (undocumented)
     → connector's own AzureServicePrincipalCredentialProvider
     → sends to login.microsoftonline.com/{tenant}/oauth2/token
     → wrapped by TokenFederationProvider

  3. auth_type="azure-sp-m2m" without azure_tenant_id (auto-discover)
     → connector calls GET {host}/aad/auth → extracts tenant from 302 redirect
     → then same as path 2

U2M Paths:
  4. auth_type="databricks-oauth" (documented for non-Azure)
     → connector's DatabricksOAuthProvider with InHouseOAuthEndpointCollection
     → opens browser to {host}/oidc/oauth2/v2.0/authorize

  5. auth_type="azure-oauth" (Azure-specific U2M)
     → connector's DatabricksOAuthProvider with AzureOAuthEndpointCollection
     → opens browser to {host}/oidc/oauth2/v2.0/authorize with Azure scopes

  6. No auth params at all (default — falls through to U2M)
     → same as path 4 if oauth_client_id and redirect_port are configured

Workspaces:
  Staging SPOG:   dogfood-spog.staging.azuredatabricks.net
  Staging Legacy: adb-7064161269814046.2.staging.azuredatabricks.net
  Prod Legacy:    adb-6436897454825492.12.azuredatabricks.net

Usage:
    source ~/.zshrc
    cd databricks-sql-python
    PYTHONPATH=src .venv/bin/python examples/spog_all_auth_test.py
"""

import json
import logging
import os
import sys
import time
from typing import Optional

from databricks import sql
from databricks.sdk.core import oauth_service_principal, Config

# Enable debug logging to capture auth flow details
LOG_FILE = "/tmp/python_spog_auth_debug.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w"),
    ],
)
# Suppress noisy libraries but keep auth-related ones
for name in ["urllib3", "urllib3.connectionpool", "urllib3.response", "thrift", "pyarrow"]:
    logging.getLogger(name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

STG_SPOG = "dogfood-spog.staging.azuredatabricks.net"
STG_LEGACY = "adb-7064161269814046.2.staging.azuredatabricks.net"
STG_WH = "/sql/1.0/warehouses/e256699345d1ac74"
STG_SPOG_PATH = STG_WH + "?o=7064161269814046"
STG_TENANT = "e3fe3f22-4b98-4c04-82cc-d8817d1b17da"

PROD_SPOG = "peco.azuredatabricks.net"
PROD_LEGACY = "adb-6436897454825492.12.azuredatabricks.net"
PROD_WH = "/sql/1.0/warehouses/00adc7b6c00429b8"
PROD_SPOG_PATH = PROD_WH + "?o=6436897454825492"
PROD_TENANT = "9f37a392-f0ae-4280-9796-f1864a10effc"

# Credentials
STG_PAT = os.getenv("DATABRICKS_DOGFOOD_WESTUS_STAGING_TOKEN")

# Staging: Databricks managed SP (dose secret) — for SDK M2M
STG_M2M_ID = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")          # dc8dd813
STG_M2M_SEC = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")     # dose...

# Staging: Azure AD app (Azure Portal secret) — for azure-sp-m2m
STG_ENTRA_ID = os.getenv("DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID")      # d7f11108
STG_ENTRA_SEC = os.getenv("DATABRICKS_SPOG_ENTRA_TEST_CLIENT_SECRET") # eAA8...

# Prod
PROD_PAT = os.getenv("DATABRICKS_PECOTESTING_TOKEN_PERSONAL")

# Prod: Entra managed SP (dose secret) — for SDK M2M
PROD_M2M_ID = os.getenv("DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_ID_MSR_SPN")   # a6f72159
PROD_M2M_SEC = os.getenv("DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_SECRET_MSR_SPN")

# Prod: Azure AD app (Azure Portal secret) — for azure-sp-m2m
PROD_ENTRA_ID = os.getenv("DATABRICKS_AAD_CLIENT_ID")                 # d154b9ed
PROD_ENTRA_SEC = os.getenv("DATABRICKS_AAD_CLIENT_SECRET")            # US28...

U2M_ENABLED = os.getenv("DATABRICKS_SPOG_U2M_ENABLED") == "true"

# ============================================================================
# Test infrastructure
# ============================================================================

results = []
COL = 65


def record(name, status, elapsed=0, error=None):
    results.append((name, status, elapsed, error))
    if sys.stdout.isatty():
        tag = {"PASS": "\033[32mPASS\033[0m", "FAIL": "\033[31mFAIL\033[0m",
               "SKIP": "\033[33mSKIP\033[0m"}.get(status, status)
    else:
        tag = status
    t = f"({elapsed:.1f}s)" if elapsed > 0 else ""
    print(f"  {name:<{COL}}{tag}  {t}")
    if error:
        print(f"    -> {str(error)[:300]}")
    logger.info(f"TEST {status}: {name} {f'error={error}' if error else ''}")


def run(name, **kw):
    """Connect, SELECT 1, verify, close."""
    logger.info(f"=== START TEST: {name} ===")
    logger.info(f"  connect kwargs: { {k: ('***' if 'secret' in k.lower() or 'token' in k.lower() else v) for k,v in kw.items()} }")
    t0 = time.time()
    try:
        conn = sql.connect(**kw)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        el = time.time() - t0
        if row and row[0] == 1:
            record(name, "PASS", el)
        else:
            record(name, "FAIL", el, f"Expected 1, got {row}")
    except Exception as e:
        el = time.time() - t0
        record(name, "FAIL", el, f"{type(e).__name__}: {e}")
    logger.info(f"=== END TEST: {name} ({time.time()-t0:.1f}s) ===\n")


def skip(name, reason):
    record(name, "SKIP", error=reason)


def section(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")
    logger.info(f"\n{'='*60}\n  {title}\n{'='*60}")


def make_sdk_provider(host, cid, csec):
    """Build credentials_provider using databricks-sdk oauth_service_principal.
    This is the documented M2M path from:
      https://docs.databricks.com/en/dev-tools/python-sql-connector.html
    and examples/m2m_oauth.py
    """
    def credential_provider():
        config = Config(host=f"https://{host}", client_id=cid, client_secret=csec)
        return oauth_service_principal(config)
    return credential_provider


# ============================================================================
# M2M Tests
# ============================================================================

def test_m2m_path1_sdk():
    """Path 1: SDK M2M via credentials_provider (documented).
    Code: auth.py:18-19 → ExternalAuthProvider(credentials_provider)
    Token from: SDK → OIDC discovery → {host}/oidc/v1/token
    """
    section("M2M Path 1: SDK credentials_provider (documented)")
    print("  Code: databricks.sdk.core.oauth_service_principal → {host}/oidc/v1/token")
    print()

    # Staging
    if STG_M2M_ID and STG_M2M_SEC:
        run("Path1 SDK M2M (dc8dd813) | Stg SPOG",
            server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
            credentials_provider=make_sdk_provider(STG_SPOG, STG_M2M_ID, STG_M2M_SEC))
        run("Path1 SDK M2M (dc8dd813) | Stg Legacy",
            server_hostname=STG_LEGACY, http_path=STG_WH,
            credentials_provider=make_sdk_provider(STG_LEGACY, STG_M2M_ID, STG_M2M_SEC))
    else:
        skip("Path1 SDK M2M | Stg", "DATABRICKS_DOGFOOD_AZURE_CLIENT_ID/SECRET not set")

    # Prod
    if PROD_M2M_ID and PROD_M2M_SEC:
        run("Path1 SDK M2M (a6f72159) | Prod Legacy",
            server_hostname=PROD_LEGACY, http_path=PROD_WH,
            credentials_provider=make_sdk_provider(PROD_LEGACY, PROD_M2M_ID, PROD_M2M_SEC))
    else:
        skip("Path1 SDK M2M | Prod", "DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_ID_MSR_SPN not set")


def test_m2m_path2_azure_sp_explicit():
    """Path 2: auth_type='azure-sp-m2m' with explicit tenant (undocumented).
    Code: auth.py:20-30 → AzureServicePrincipalCredentialProvider
    Token from: login.microsoftonline.com/{tenant}/oauth2/token
    """
    section("M2M Path 2: azure-sp-m2m + explicit tenant (undocumented)")
    print("  Code: authenticators.py:161 AzureServicePrincipalCredentialProvider")
    print("  Token from: login.microsoftonline.com/{tenant}/oauth2/token")
    print()

    # Staging: d7f11108 in staging tenant
    if STG_ENTRA_ID and STG_ENTRA_SEC:
        run("Path2 AzureSP (d7f11108+tenant) | Stg SPOG",
            server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
            auth_type="azure-sp-m2m",
            azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC,
            azure_tenant_id=STG_TENANT)
        run("Path2 AzureSP (d7f11108+tenant) | Stg Legacy",
            server_hostname=STG_LEGACY, http_path=STG_WH,
            auth_type="azure-sp-m2m",
            azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC,
            azure_tenant_id=STG_TENANT)
    else:
        skip("Path2 AzureSP | Stg", "DATABRICKS_SPOG_ENTRA_TEST not set")

    # Prod: d154b9ed in prod tenant
    if PROD_ENTRA_ID and PROD_ENTRA_SEC:
        run("Path2 AzureSP (d154b9ed+tenant) | Prod Legacy",
            server_hostname=PROD_LEGACY, http_path=PROD_WH,
            auth_type="azure-sp-m2m",
            azure_client_id=PROD_ENTRA_ID, azure_client_secret=PROD_ENTRA_SEC,
            azure_tenant_id=PROD_TENANT)
        run("Path2 AzureSP (d154b9ed+tenant) | Prod SPOG",
            server_hostname=PROD_SPOG, http_path=PROD_SPOG_PATH,
            auth_type="azure-sp-m2m",
            azure_client_id=PROD_ENTRA_ID, azure_client_secret=PROD_ENTRA_SEC,
            azure_tenant_id=PROD_TENANT)
    else:
        skip("Path2 AzureSP | Prod", "DATABRICKS_AAD_CLIENT not set")


def test_m2m_path3_azure_sp_auto():
    """Path 3: auth_type='azure-sp-m2m' without tenant (auto-discover).
    Code: authenticators.py:201 → get_azure_tenant_id_from_host(hostname)
          common.py:106 → GET {host}/aad/auth → extracts tenant from 302 redirect
    """
    section("M2M Path 3: azure-sp-m2m + auto-discover tenant (undocumented)")
    print("  Code: common.py:106 get_azure_tenant_id_from_host → GET {host}/aad/auth")
    print()

    # Staging
    if STG_ENTRA_ID and STG_ENTRA_SEC:
        run("Path3 AzureSP auto-tenant (d7f11108) | Stg SPOG",
            server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
            auth_type="azure-sp-m2m",
            azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC)
        run("Path3 AzureSP auto-tenant (d7f11108) | Stg Legacy",
            server_hostname=STG_LEGACY, http_path=STG_WH,
            auth_type="azure-sp-m2m",
            azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC)
    else:
        skip("Path3 AzureSP auto | Stg", "DATABRICKS_SPOG_ENTRA_TEST not set")

    # Prod
    if PROD_ENTRA_ID and PROD_ENTRA_SEC:
        run("Path3 AzureSP auto-tenant (d154b9ed) | Prod Legacy",
            server_hostname=PROD_LEGACY, http_path=PROD_WH,
            auth_type="azure-sp-m2m",
            azure_client_id=PROD_ENTRA_ID, azure_client_secret=PROD_ENTRA_SEC)
        run("Path3 AzureSP auto-tenant (d154b9ed) | Prod SPOG",
            server_hostname=PROD_SPOG, http_path=PROD_SPOG_PATH,
            auth_type="azure-sp-m2m",
            azure_client_id=PROD_ENTRA_ID, azure_client_secret=PROD_ENTRA_SEC)
    else:
        skip("Path3 AzureSP auto | Prod", "DATABRICKS_AAD_CLIENT not set")


# ============================================================================
# U2M Tests
# ============================================================================

def test_u2m_path4_databricks_oauth():
    """Path 4: auth_type='databricks-oauth' (documented U2M).
    Code: auth.py:31-44 → DatabricksOAuthProvider
          endpoint.py: InHouseOAuthEndpointCollection (on Azure .azuredatabricks.net)
          Opens browser to {host}/oidc/oauth2/v2.0/authorize
    Scopes: ["sql", "offline_access"]
    """
    section("U2M Path 4: auth_type='databricks-oauth' (documented)")
    print("  Code: authenticators.py:56 DatabricksOAuthProvider → browser flow")
    print("  Scopes: sql offline_access")
    print()

    if not U2M_ENABLED:
        skip("Path4 U2M databricks-oauth | Stg SPOG", "DATABRICKS_SPOG_U2M_ENABLED != true")
        skip("Path4 U2M databricks-oauth | Stg Legacy", "DATABRICKS_SPOG_U2M_ENABLED != true")
        return

    run("Path4 U2M databricks-oauth | Stg SPOG",
        server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
        auth_type="databricks-oauth")
    run("Path4 U2M databricks-oauth | Stg Legacy",
        server_hostname=STG_LEGACY, http_path=STG_WH,
        auth_type="databricks-oauth")


def test_u2m_path5_azure_oauth():
    """Path 5: auth_type='azure-oauth' (Azure U2M).
    Code: auth.py:31-44 → DatabricksOAuthProvider
          endpoint.py: AzureOAuthEndpointCollection
          Opens browser to {host}/oidc/oauth2/v2.0/authorize
    Scopes: ["2ff814a6.../user_impersonation", "offline_access"]
    """
    section("U2M Path 5: auth_type='azure-oauth'")
    print("  Code: authenticators.py:56 DatabricksOAuthProvider → browser flow")
    print("  Scopes: 2ff814a6.../user_impersonation offline_access")
    print()

    if not U2M_ENABLED:
        skip("Path5 U2M azure-oauth | Stg SPOG", "DATABRICKS_SPOG_U2M_ENABLED != true")
        skip("Path5 U2M azure-oauth | Stg Legacy", "DATABRICKS_SPOG_U2M_ENABLED != true")
        return

    run("Path5 U2M azure-oauth | Stg SPOG",
        server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
        auth_type="azure-oauth")
    run("Path5 U2M azure-oauth | Stg Legacy",
        server_hostname=STG_LEGACY, http_path=STG_WH,
        auth_type="azure-oauth")


def test_u2m_path6_default():
    """Path 6: No auth params — connector falls through to U2M.
    Code: auth.py:51-64 → DatabricksOAuthProvider (fallback)
    Only if oauth_client_id and oauth_redirect_port_range are configured.
    In practice, this opens a browser (same as databricks-oauth).
    """
    section("U2M Path 6: No auth params (default fallback)")
    print("  Code: auth.py:51-64 → falls through to DatabricksOAuthProvider")
    print()

    if not U2M_ENABLED:
        skip("Path6 U2M default | Stg Legacy", "DATABRICKS_SPOG_U2M_ENABLED != true")
        return

    run("Path6 U2M default | Stg Legacy",
        server_hostname=STG_LEGACY, http_path=STG_WH)


# ============================================================================
# PAT (baseline)
# ============================================================================

def test_pat():
    section("PAT (baseline)")

    if STG_PAT:
        run("PAT | Stg SPOG",
            server_hostname=STG_SPOG, http_path=STG_SPOG_PATH, access_token=STG_PAT)
        run("PAT | Stg Legacy",
            server_hostname=STG_LEGACY, http_path=STG_WH, access_token=STG_PAT)
    else:
        skip("PAT | Stg", "DATABRICKS_DOGFOOD_WESTUS_STAGING_TOKEN not set")

    if PROD_PAT:
        run("PAT | Prod Legacy",
            server_hostname=PROD_LEGACY, http_path=PROD_WH, access_token=PROD_PAT)
        run("PAT | Prod SPOG",
            server_hostname=PROD_SPOG, http_path=PROD_SPOG_PATH, access_token=PROD_PAT)
    else:
        skip("PAT | Prod", "DATABRICKS_PECOTESTING_TOKEN_PERSONAL not set")


# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("=" * 80)
    print("  Python SQL Connector — Complete Auth Flow Test")
    print(f"  Debug log: {LOG_FILE}")
    print("=" * 80)
    print()
    print("  Credentials:")
    print(f"    STG PAT ................. {'YES' if STG_PAT else 'NO'}")
    print(f"    STG SDK M2M (dc8dd813) .. {'YES' if STG_M2M_ID else 'NO'}  (dose → Databricks OIDC)")
    print(f"    STG Entra (d7f11108) .... {'YES' if STG_ENTRA_ID else 'NO'}  (Azure secret → Azure AD)")
    print(f"    PROD PAT ................ {'YES' if PROD_PAT else 'NO'}")
    print(f"    PROD SDK M2M (a6f72159) . {'YES' if PROD_M2M_ID else 'NO'}  (dose → Databricks OIDC)")
    print(f"    PROD Entra (d154b9ed) ... {'YES' if PROD_ENTRA_ID else 'NO'}  (Azure secret → Azure AD)")
    print(f"    U2M (browser) ........... {'YES' if U2M_ENABLED else 'NO (set DATABRICKS_SPOG_U2M_ENABLED=true)'}")

    # Run all tests
    test_pat()
    test_m2m_path1_sdk()
    test_m2m_path2_azure_sp_explicit()
    test_m2m_path3_azure_sp_auto()
    test_u2m_path4_databricks_oauth()
    test_u2m_path5_azure_oauth()
    test_u2m_path6_default()

    # Summary
    print(f"\n{'='*80}")
    print("  SUMMARY")
    print(f"{'='*80}")
    p = sum(1 for _, s, _, _ in results if s == "PASS")
    f = sum(1 for _, s, _, _ in results if s == "FAIL")
    sk = sum(1 for _, s, _, _ in results if s == "SKIP")
    print(f"  Total: {len(results)}  |  PASS: {p}  |  FAIL: {f}  |  SKIP: {sk}")

    if f:
        print(f"\n  FAILURES:")
        for n, s, _, e in results:
            if s == "FAIL":
                print(f"    - {n}")
                if e:
                    print(f"      {str(e)[:250]}")

    print(f"\n  Debug log: {LOG_FILE}")
    print()
    sys.exit(1 if f else 0)


if __name__ == "__main__":
    main()
