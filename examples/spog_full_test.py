"""
Full SPOG (Single Panel of Glass) E2E test suite for Databricks SQL Python Connector.

Targets an Azure production workspace.

Tests:
  1. PAT authentication (Thrift + SEA)
  2. Databricks M2M OAuth (Thrift + SEA)
  3. U2M browser OAuth (skip if env not set)
  4. Azure Entra ID M2M (always SKIP)
  5. Telemetry endpoint (direct HTTP POST, verify 200)
  6. Feature flags endpoint (direct HTTP GET, verify 200)

Workspace:
  Host: adb-2220717038207243.3.azuredatabricks.net
  Warehouse: /sql/1.0/warehouses/f45eefbaf9e766bd
  Workspace ID: 2220717038207243

Env vars:
  DATABRICKS_DOGFOOD_SPOG_TOKEN              -- PAT
  DATABRICKS_DOGFOOD_SPOG_CLIENT_ID          -- Databricks M2M client ID
  DATABRICKS_DOGFOOD_SPOG_CLIENT_SECRET      -- Databricks M2M client secret
  DATABRICKS_SPOG_U2M_ENABLED=true           -- Enable U2M browser tests

Usage:
    cd /path/to/databricks-sql-python
    PYTHONPATH=src .venv/bin/python examples/spog_full_test.py
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

HOST = "adb-2220717038207243.3.azuredatabricks.net"
WAREHOUSE_PATH = "/sql/1.0/warehouses/f45eefbaf9e766bd"
WORKSPACE_ID = "2220717038207243"

FF_DRIVER = "PYTHON"
FF_VERSION = "4.2.5"

# Credentials from env
PAT = os.getenv("DATABRICKS_DOGFOOD_SPOG_TOKEN")

M2M_CLIENT_ID = os.getenv("DATABRICKS_DOGFOOD_SPOG_CLIENT_ID")
M2M_CLIENT_SECRET = os.getenv("DATABRICKS_DOGFOOD_SPOG_CLIENT_SECRET")

U2M_ENABLED = os.getenv("DATABRICKS_SPOG_U2M_ENABLED") == "true"

# Counters
passed = 0
failed = 0
skipped = 0
results = []

# Disable urllib3 SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Workspace-level OIDC credentials provider (no databricks-sdk)
# ---------------------------------------------------------------------------


def _workspace_oidc_token(host, client_id, client_secret):
    """
    Fetch an OAuth token from the workspace-level OIDC endpoint.
    POST https://{host}/oidc/v1/token with client_credentials grant.
    Returns the access_token string.
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
            f"OIDC token request to {url} failed with status {resp.status}: "
            f"{resp.data.decode('utf-8', errors='replace')[:300]}"
        )
    data = json.loads(resp.data.decode("utf-8"))
    return data["access_token"]


def make_databricks_m2m_provider(host, client_id, client_secret):
    """
    Build a credentials_provider callable for the Python SQL connector.
    The connector calls credentials_provider() to get a HeaderFactory,
    and then calls HeaderFactory() to get a dict of HTTP headers.
    """

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
             use_sea=False, auth_type=None, extra_kwargs=None):
    """
    Connect, run SELECT 1, verify result == 1, close.
    """
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
        if use_sea:
            kwargs["use_sea"] = True
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
        err_msg = f"{type(e).__name__}: {str(e)[:200]}"
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

def test_telemetry():
    """Direct HTTP POST to /telemetry-ext, verify HTTP 200."""
    section("Telemetry Endpoint")
    name = "Telemetry | POST /telemetry-ext"
    print(f"  {name:<{COL_WIDTH}}", end="", flush=True)

    if not PAT:
        print("SKIP  (DATABRICKS_DOGFOOD_SPOG_TOKEN not set)")
        _record(name, "SKIP", 0.0, "DATABRICKS_DOGFOOD_SPOG_TOKEN not set")
        return

    t0 = time.time()
    try:
        http = urllib3.PoolManager()
        resp = http.request(
            "POST",
            f"https://{HOST}/telemetry-ext",
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


def test_feature_flags():
    """Direct HTTP GET to /api/2.0/connector-service/feature-flags/{DRIVER}/{VERSION}, verify HTTP 200."""
    section("Feature Flags Endpoint")
    path = f"/api/2.0/connector-service/feature-flags/{FF_DRIVER}/{FF_VERSION}"
    name = f"Feature Flags | GET {path}"
    print(f"  {name:<{COL_WIDTH}}", end="", flush=True)

    if not PAT:
        print("SKIP  (DATABRICKS_DOGFOOD_SPOG_TOKEN not set)")
        _record(name, "SKIP", 0.0, "DATABRICKS_DOGFOOD_SPOG_TOKEN not set")
        return

    t0 = time.time()
    try:
        http = urllib3.PoolManager()
        resp = http.request(
            "GET",
            f"https://{HOST}{path}",
            headers={
                "Authorization": f"Bearer {PAT}",
            },
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

def test_pat():
    """PAT auth, Thrift and SEA."""
    section("PAT Authentication")
    if not PAT:
        for label in [
            "PAT | Thrift",
            "PAT | SEA",
        ]:
            skip_test(label, "DATABRICKS_DOGFOOD_SPOG_TOKEN not set")
        return

    run_test("PAT | Thrift", HOST, WAREHOUSE_PATH, token=PAT)
    run_test("PAT | SEA", HOST, WAREHOUSE_PATH, token=PAT, use_sea=True)


def test_databricks_m2m():
    """Databricks M2M (workspace OIDC), Thrift and SEA."""
    section("Databricks M2M OAuth")
    have_creds = M2M_CLIENT_ID and M2M_CLIENT_SECRET
    if not have_creds:
        for label in [
            "Databricks M2M | Thrift",
            "Databricks M2M | SEA",
        ]:
            skip_test(label, "DATABRICKS_DOGFOOD_SPOG_CLIENT_ID/SECRET not set")
        return

    provider = make_databricks_m2m_provider(HOST, M2M_CLIENT_ID, M2M_CLIENT_SECRET)

    run_test("Databricks M2M | Thrift", HOST, WAREHOUSE_PATH, credentials_provider=provider)
    run_test("Databricks M2M | SEA", HOST, WAREHOUSE_PATH, credentials_provider=provider, use_sea=True)


def test_u2m():
    """U2M (browser flow), Thrift only."""
    section("U2M (Browser) OAuth")
    if not U2M_ENABLED:
        skip_test("U2M | Thrift", "DATABRICKS_SPOG_U2M_ENABLED != true")
        return

    run_test("U2M | Thrift", HOST, WAREHOUSE_PATH)


def test_entra_id_m2m():
    """Azure Entra ID M2M -- always SKIP."""
    section("Azure Entra ID M2M")
    skip_test("Entra ID M2M | Thrift", "Entra ID credentials not configured")
    skip_test("Entra ID M2M | SEA", "Entra ID credentials not configured")


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
    print("  Databricks SQL Python Connector -- Azure SPOG E2E Test Suite")
    print(f"  Host: {HOST}")
    print(f"  Warehouse: {WAREHOUSE_PATH}")
    print("=" * 80)

    # Show which credentials are available
    print(f"\n  Credentials detected:")
    print(f"    PAT .......................... {'YES' if PAT else 'NO'}")
    print(f"    Databricks M2M .............. {'YES' if (M2M_CLIENT_ID and M2M_CLIENT_SECRET) else 'NO'}")
    print(f"    U2M (browser) ............... {'YES' if U2M_ENABLED else 'NO'}")
    print(f"    Entra ID M2M ................ SKIP (not configured)")

    # Run all test groups
    test_pat()
    test_databricks_m2m()
    test_u2m()
    test_entra_id_m2m()
    test_telemetry()
    test_feature_flags()

    # Print summary table
    print_summary()

    # Exit code
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
