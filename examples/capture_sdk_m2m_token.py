"""Capture the SDK-issued M2M JWT on Staging SPOG vs Staging Legacy, decode claims, print side by side.

Isolates the SDK credential flow — no pysql connector involvement — so the output
shows exactly what the server issues in response to the SDK's M2M request.
"""
import base64
import json
import os
import sys
import urllib.request

from databricks.sdk.core import Config, oauth_service_principal
import databricks.sdk as _sdk

print(f"databricks-sdk version: {_sdk.version.__version__}\n")

CLIENT_ID = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")
if not (CLIENT_ID and CLIENT_SECRET):
    print("Missing DATABRICKS_DOGFOOD_AZURE_CLIENT_ID/SECRET")
    sys.exit(1)


def decode_jwt(token: str):
    """Decode a JWT's header + payload (no signature verification) and return dicts."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("not a JWT")

    def b64url_decode(s):
        s += "=" * (-len(s) % 4)
        return json.loads(base64.urlsafe_b64decode(s))

    return b64url_decode(parts[0]), b64url_decode(parts[1])


def fetch_well_known(host):
    """Print the /.well-known/databricks-config response so we see the metadata the SDK consumes."""
    try:
        r = urllib.request.urlopen(f"https://{host}/.well-known/databricks-config", timeout=10)
        body = r.read().decode("utf-8")
        return json.loads(body)
    except Exception as e:
        return {"error": str(e)}


def capture(label, host):
    print("=" * 80)
    print(f"{label}: {host}")
    print("=" * 80)

    print("\n/.well-known/databricks-config:")
    print(json.dumps(fetch_well_known(host), indent=2))

    cfg = Config(host=f"https://{host}", client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    provider = oauth_service_principal(cfg)
    headers = provider()
    authz = headers.get("Authorization", "")
    if not authz.startswith("Bearer "):
        print(f"\nNo bearer token in headers: {headers!r}")
        return
    token = authz[len("Bearer ") :]

    print(f"\nAccess token (first 32 chars): {token[:32]}...")
    hdr, payload = decode_jwt(token)
    print("\nJWT header:")
    print(json.dumps(hdr, indent=2))
    print("\nJWT payload (claims):")
    print(json.dumps(payload, indent=2))
    print()


capture("Staging SPOG", "dogfood-spog.staging.azuredatabricks.net")
capture("Staging Legacy", "adb-7064161269814046.2.staging.azuredatabricks.net")
