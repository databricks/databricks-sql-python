"""Trace every HTTP request the SDK makes while fetching an M2M token on Staging SPOG.

Monkey-patches urllib3 to log outbound requests + response status, so we see
the exact endpoint chain the SDK walks through to produce the captured token.
"""
import json
import logging
import os
import sys
import urllib.request

from databricks.sdk.core import Config, oauth_service_principal

# Enable verbose SDK logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")
for name in ["databricks.sdk", "urllib3"]:
    logging.getLogger(name).setLevel(logging.DEBUG)

# Intercept HTTP calls with an adapter-level hook
import requests
_orig_send = requests.adapters.HTTPAdapter.send

call_num = [0]
def traced_send(self, req, **kw):
    call_num[0] += 1
    body_preview = ""
    if req.body:
        try:
            body = req.body if isinstance(req.body, str) else req.body.decode("utf-8", errors="replace")
            # Redact secrets so this output is pasteable
            for secret_key in ("client_secret=", "assertion="):
                if secret_key in body:
                    idx = body.find(secret_key) + len(secret_key)
                    amp = body.find("&", idx)
                    end = amp if amp >= 0 else len(body)
                    body = body[:idx] + "***REDACTED***" + body[end:]
            body_preview = f"\n    body: {body}"
        except Exception:
            body_preview = f"\n    body: <binary, {len(req.body)} bytes>"
    print(f"\n[{call_num[0]}] → {req.method} {req.url}{body_preview}")
    resp = _orig_send(self, req, **kw)
    print(f"    ← {resp.status_code} {resp.reason}")
    ctype = resp.headers.get("content-type", "")
    if "json" in ctype and resp.content:
        try:
            body = json.loads(resp.content)
            # Truncate long values
            for k, v in list(body.items()):
                if isinstance(v, str) and len(v) > 120:
                    body[k] = v[:120] + "...<truncated>"
                if isinstance(v, list) and len(v) > 8:
                    body[k] = v[:8] + ["...<truncated>"]
            print(f"    response JSON: {json.dumps(body, indent=6)[:1400]}")
        except Exception:
            pass
    return resp

requests.adapters.HTTPAdapter.send = traced_send


CLIENT_ID = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")
if not (CLIENT_ID and CLIENT_SECRET):
    print("Missing creds")
    sys.exit(1)

HOST = sys.argv[1] if len(sys.argv) > 1 else "dogfood-spog.staging.azuredatabricks.net"

print(f"\n========  Tracing SDK M2M flow against: {HOST}  ========\n")

cfg = Config(host=f"https://{HOST}", client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
print("\n>>> Config constructed, now calling oauth_service_principal()\n")
provider = oauth_service_principal(cfg)
print("\n>>> Provider created, now calling it to get headers\n")
headers = provider()
print("\n>>> Got headers (Authorization redacted in summary below)\n")

# Print the final Authorization header preview
auth = headers.get("Authorization", "")
print(f"Final Authorization: {auth[:32]}...{auth[-16:]}")
