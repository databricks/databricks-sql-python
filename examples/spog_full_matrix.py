"""
Python SQL Connector — Full SPOG Matrix Test

Tests all auth flows on BOTH staging and prod workspaces with correct credentials.

Usage:
    source ~/.zshrc
    cd databricks-sql-python
    PYTHONPATH=src .venv/bin/python examples/spog_full_matrix.py
"""

import os, sys, time
from databricks import sql
from databricks.sdk.core import oauth_service_principal, Config

# === Workspaces ===
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

# === Credentials ===
STG_PAT = os.getenv("DATABRICKS_DOGFOOD_WESTUS_STAGING_TOKEN")
STG_M2M_ID = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")
STG_M2M_SEC = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")
STG_ENTRA_ID = os.getenv("DATABRICKS_SPOG_ENTRA_TEST_CLIENT_ID")       # d7f11108, staging tenant
STG_ENTRA_SEC = os.getenv("DATABRICKS_SPOG_ENTRA_TEST_CLIENT_SECRET")

PROD_PAT = os.getenv("DATABRICKS_PECOTESTING_TOKEN_PERSONAL")
PROD_M2M_ID = os.getenv("DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_ID_MSR_SPN")  # a6f72159
PROD_M2M_SEC = os.getenv("DATABRICKS_PECOTESTING_DATABRICKS_CLIENT_SECRET_MSR_SPN")
PROD_ENTRA_ID = os.getenv("DATABRICKS_AAD_CLIENT_ID")                  # d154b9ed, prod tenant
PROD_ENTRA_SEC = os.getenv("DATABRICKS_AAD_CLIENT_SECRET")

results = []
COL = 65

def record(name, status, elapsed=0, error=None):
    results.append((name, status, elapsed, error))
    tag = {"PASS": "\033[32mPASS\033[0m", "FAIL": "\033[31mFAIL\033[0m", "SKIP": "\033[33mSKIP\033[0m"}.get(status, status)
    t = f"({elapsed:.1f}s)" if elapsed > 0 else ""
    print(f"  {name:<{COL}}{tag}  {t}")
    if error: print(f"    -> {str(error)[:250]}")

def run(name, **kw):
    t0 = time.time()
    try:
        conn = sql.connect(**kw)
        cur = conn.cursor(); cur.execute("SELECT 1"); row = cur.fetchone(); cur.close(); conn.close()
        el = time.time() - t0
        record(name, "PASS" if row and row[0] == 1 else "FAIL", el, None if row and row[0] == 1 else f"got {row}")
    except Exception as e:
        record(name, "FAIL", time.time() - t0, f"{type(e).__name__}: {e}")

def skip(name, reason): record(name, "SKIP", error=reason)

def section(t): print(f"\n{'='*80}\n  {t}\n{'='*80}")

def make_sdk_provider(host, cid, csec):
    def p():
        return oauth_service_principal(Config(host=f"https://{host}", client_id=cid, client_secret=csec))
    return p

# ============================================================
# STAGING
# ============================================================
def test_staging():
    section("STAGING SPOG — dogfood-spog.staging.azuredatabricks.net")
    print(f"  Tenant: {STG_TENANT}\n")

    # PAT
    if STG_PAT:
        run("Stg | PAT | SPOG", server_hostname=STG_SPOG, http_path=STG_SPOG_PATH, access_token=STG_PAT)
        run("Stg | PAT | Legacy", server_hostname=STG_LEGACY, http_path=STG_WH, access_token=STG_PAT)
    else: skip("Stg | PAT", "no token")

    # SDK M2M (Databricks OIDC)
    if STG_M2M_ID and STG_M2M_SEC:
        run("Stg | SDK M2M (dc8dd813) | SPOG", server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
            credentials_provider=make_sdk_provider(STG_SPOG, STG_M2M_ID, STG_M2M_SEC))
        run("Stg | SDK M2M (dc8dd813) | Legacy", server_hostname=STG_LEGACY, http_path=STG_WH,
            credentials_provider=make_sdk_provider(STG_LEGACY, STG_M2M_ID, STG_M2M_SEC))
    else: skip("Stg | SDK M2M", "no creds")

    # Azure AD M2M (d7f11108, staging tenant)
    if STG_ENTRA_ID and STG_ENTRA_SEC:
        run("Stg | Azure AD (d7f11108, explicit tenant) | SPOG", server_hostname=STG_SPOG, http_path=STG_SPOG_PATH,
            auth_type="azure-sp-m2m", azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC, azure_tenant_id=STG_TENANT)
        run("Stg | Azure AD (d7f11108, explicit tenant) | Legacy", server_hostname=STG_LEGACY, http_path=STG_WH,
            auth_type="azure-sp-m2m", azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC, azure_tenant_id=STG_TENANT)
        run("Stg | Azure AD (d7f11108, auto-tenant) | Legacy", server_hostname=STG_LEGACY, http_path=STG_WH,
            auth_type="azure-sp-m2m", azure_client_id=STG_ENTRA_ID, azure_client_secret=STG_ENTRA_SEC)
    else: skip("Stg | Azure AD", "no creds")

# ============================================================
# PROD
# ============================================================
def test_prod():
    section("PROD — peco.azuredatabricks.net")
    print(f"  Tenant: {PROD_TENANT}\n")

    # PAT
    if PROD_PAT:
        run("Prod | PAT | Legacy", server_hostname=PROD_LEGACY, http_path=PROD_WH, access_token=PROD_PAT)
    else: skip("Prod | PAT", "no token")

    # SDK M2M (a6f72159, Databricks OIDC)
    if PROD_M2M_ID and PROD_M2M_SEC:
        run("Prod | SDK M2M (a6f72159) | Legacy", server_hostname=PROD_LEGACY, http_path=PROD_WH,
            credentials_provider=make_sdk_provider(PROD_LEGACY, PROD_M2M_ID, PROD_M2M_SEC))
    else: skip("Prod | SDK M2M", "no creds")

    # Azure AD M2M (d154b9ed, prod tenant)
    if PROD_ENTRA_ID and PROD_ENTRA_SEC:
        run("Prod | Azure AD (d154b9ed, explicit tenant) | Legacy", server_hostname=PROD_LEGACY, http_path=PROD_WH,
            auth_type="azure-sp-m2m", azure_client_id=PROD_ENTRA_ID, azure_client_secret=PROD_ENTRA_SEC, azure_tenant_id=PROD_TENANT)
        run("Prod | Azure AD (d154b9ed, auto-tenant) | Legacy", server_hostname=PROD_LEGACY, http_path=PROD_WH,
            auth_type="azure-sp-m2m", azure_client_id=PROD_ENTRA_ID, azure_client_secret=PROD_ENTRA_SEC)
    else: skip("Prod | Azure AD", "no creds")

    # SDK M2M on prod with Entra managed SP dose secret (a6f72159) — via credentials_provider
    if PROD_M2M_ID and PROD_M2M_SEC:
        run("Prod | SDK M2M (a6f72159, dose) via provider | Legacy", server_hostname=PROD_LEGACY, http_path=PROD_WH,
            credentials_provider=make_sdk_provider(PROD_LEGACY, PROD_M2M_ID, PROD_M2M_SEC))
    else: skip("Prod | SDK M2M provider", "no creds")

# ============================================================
def main():
    print("\n" + "="*80)
    print("  Python SQL Connector — Full SPOG Matrix")
    print("="*80)
    test_staging()
    test_prod()
    print(f"\n{'='*80}\n  SUMMARY\n{'='*80}")
    p = sum(1 for _,s,_,_ in results if s=="PASS")
    f = sum(1 for _,s,_,_ in results if s=="FAIL")
    sk = sum(1 for _,s,_,_ in results if s=="SKIP")
    print(f"  Total: {len(results)}  |  PASS: {p}  |  FAIL: {f}  |  SKIP: {sk}")
    if f:
        print(f"\n  FAILURES:")
        for n,s,_,e in results:
            if s=="FAIL": print(f"    - {n}"); e and print(f"      {str(e)[:200]}")
    print()
    sys.exit(1 if f else 0)

if __name__ == "__main__": main()
