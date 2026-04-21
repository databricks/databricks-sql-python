"""Isolate server vs connector: get an SDK-issued M2M token on Stg SPOG,
then POST directly to the Thrift endpoint with it — skip pysql's TokenFederationProvider.

If Thrift returns 200 → server DOES accept the accounts-shaped iss → the
previous failure was the pysql connector's fault (TokenFederationProvider trying to
exchange unnecessarily) — not the server's.

If Thrift returns 4xx → server rejects the accounts-shaped iss → SPOG team's
claim that it works is inconsistent with what we observe on this workspace.
"""
import base64
import json
import os
import struct
import sys

import requests
from databricks.sdk.core import Config, oauth_service_principal

CLIENT_ID = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_DOGFOOD_AZURE_CLIENT_SECRET")

HOST = "dogfood-spog.staging.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/e256699345d1ac74?o=7064161269814046"


def decode_jwt(token):
    parts = token.split(".")
    pl = parts[1] + "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(pl))


# 1. Get an M2M token via SDK
cfg = Config(host=f"https://{HOST}", client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
provider = oauth_service_principal(cfg)
headers = provider()
token = headers["Authorization"].split(" ", 1)[1]

claims = decode_jwt(token)
print("Token claims:")
print(f"  iss: {claims.get('iss')}")
print(f"  aud: {claims.get('aud')}")
print(f"  sub: {claims.get('sub')}")
print(f"  scope: {claims.get('scope')}")
print(f"  exp: {claims.get('exp')}")
print()

# 2. Make a minimal Thrift HiveServer2 OpenSession request, directly
# (no pysql TokenFederationProvider in the way)
#
# Thrift-over-HTTP uses the TBinaryProtocol framed messages. We can use
# the generated TCLIService Python stubs that come with pysql.
sys.path.insert(0, "src")
from databricks.sql.thrift_api.TCLIService import TCLIService, ttypes
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.THttpClient import THttpClient

url = f"https://{HOST}{HTTP_PATH}"
print(f"POST target: {url}")
print()

transport = THttpClient(url)
transport.setCustomHeaders({
    "Authorization": f"Bearer {token}",
    "X-Databricks-Thrift-Version": "0",
})
protocol = TBinaryProtocol(transport)
client = TCLIService.Client(protocol)

req = ttypes.TOpenSessionReq(
    client_protocol=ttypes.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1,
    username=None,
)

print("Calling TCLIService.OpenSession directly over HTTPS...")
try:
    resp = client.OpenSession(req)
    print()
    print(f"status: {resp.status.statusCode}")
    print(f"errorMessage: {resp.status.errorMessage}")
    if resp.status.statusCode in (ttypes.TStatusCode.SUCCESS_STATUS, ttypes.TStatusCode.SUCCESS_WITH_INFO_STATUS):
        print(">>> Thrift DID accept the accounts-shaped iss token. Server is fine.")
        print(">>> The earlier failure was inside pysql's TokenFederationProvider.")
    else:
        print(">>> Thrift returned non-success status. See errorMessage + infoMessages.")
except Exception as e:
    print()
    print(f"Exception: {type(e).__name__}: {e}")
    print(">>> Server likely returned an HTTP error before Thrift got to respond.")
    # Inspect transport for last response code
    try:
        print(f"HTTP status: {transport.code}")
        print(f"HTTP headers: {dict(transport.headers) if transport.headers else {}}")
    except Exception:
        pass
