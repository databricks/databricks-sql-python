"""Minimal SDK M2M test on staging SPOG — uses latest databricks-sdk (0.102.0)."""
import os
import sys
import time
from databricks import sql
from databricks.sdk.core import oauth_service_principal, Config
import databricks.sdk as _sdk

print(f"databricks-sdk version: {_sdk.version.__version__}")

SPOG_HOST = "dogfood-spog.staging.azuredatabricks.net"
LEGACY_HOST = "adb-7064161269814046.2.staging.azuredatabricks.net"
WAREHOUSE = "/sql/1.0/warehouses/e256699345d1ac74"
SPOG_PATH = WAREHOUSE + "?o=7064161269814046"

client_id = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")
client_secret = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")
if not (client_id and client_secret):
    print("Missing DATABRICKS_DOGFOOD_AZURE_CLIENT_ID/SECRET")
    sys.exit(1)


def make_provider(hostname):
    def provider():
        cfg = Config(
            host=f"https://{hostname}",
            client_id=client_id,
            client_secret=client_secret,
        )
        return oauth_service_principal(cfg)

    return provider


def run(label, host, path):
    t0 = time.time()
    try:
        conn = sql.connect(
            server_hostname=host,
            http_path=path,
            credentials_provider=make_provider(host),
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        dt = time.time() - t0
        print(f"{label}: PASS  ({dt:.1f}s)  result={row[0]}")
    except Exception as e:
        dt = time.time() - t0
        msg = str(e)[:400]
        print(f"{label}: FAIL  ({dt:.1f}s)")
        print(f"  -> {type(e).__name__}: {msg}")


run("SDK M2M | Staging SPOG", SPOG_HOST, SPOG_PATH)
run("SDK M2M | Staging Legacy", LEGACY_HOST, WAREHOUSE)
