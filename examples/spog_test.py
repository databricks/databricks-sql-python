"""
SPOG (Single Panel of Glass) tests for Databricks SQL Python Connector.

Usage:
    cd /path/to/databricks-sql-python
    PYTHONPATH=src DATABRICKS_SPOG_TOKEN=<pat> .venv/bin/python examples/spog_test.py
"""

import json
import os
import sys
import time

from databricks import sql
from databricks.sql.common.http import HttpMethod
from databricks.sql.common.url_utils import normalize_host_with_protocol

SPOG_HOST = "e2-spog.staging.cloud.databricks.com"
LEGACY_HOST = "e2-dogfood.staging.cloud.databricks.com"
WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/dd43ee29fedd958d"
WORKSPACE_ID = "6051921418418893"
WAREHOUSE_HTTP_PATH_SPOG = f"{WAREHOUSE_HTTP_PATH}?o={WORKSPACE_ID}"
GP_CLUSTER_PATH = os.getenv("DATABRICKS_SPOG_GP_CLUSTER_PATH")
M2M_CLIENT_ID = os.getenv("DATABRICKS_SPOG_M2M_CLIENT_ID")
M2M_CLIENT_SECRET = os.getenv("DATABRICKS_SPOG_M2M_CLIENT_SECRET")
PAT = os.getenv("DATABRICKS_SPOG_TOKEN")

passed = 0
failed = 0
skipped = 0


def test(name, host, http_path, token=None, credentials_provider=None,
         use_sea=False, expect_fail=False):
    global passed, failed
    print(f"{name:<60}", end="", flush=True)
    t = time.time()
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

        conn = sql.connect(**kwargs)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS v")
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if expect_fail:
            print(f"FAIL (expected failure but got result: {row})")
            failed += 1
        elif row[0] == 1:
            print(f"PASS ({time.time() - t:.1f}s)")
            passed += 1
        else:
            print(f"FAIL (unexpected: {row})")
            failed += 1
    except Exception as e:
        if expect_fail:
            print(f"PASS (expected failure, {time.time() - t:.1f}s)")
            passed += 1
        else:
            print(f"FAIL ({time.time() - t:.1f}s)")
            print(f"  {type(e).__name__}: {str(e)[:200]}")
            failed += 1


def test_custom(name, test_fn):
    global passed, failed
    print(f"{name:<60}", end="", flush=True)
    t = time.time()
    try:
        test_fn()
        print(f"PASS ({time.time() - t:.1f}s)")
        passed += 1
    except Exception as e:
        print(f"FAIL ({time.time() - t:.1f}s)")
        print(f"  {type(e).__name__}: {str(e)[:200]}")
        failed += 1


def skip(name, reason):
    global skipped
    print(f"{name:<60}SKIPPED ({reason})")
    skipped += 1


def verify_telemetry(conn, label=""):
    """Directly hit the telemetry endpoint using the connection's auth and assert 200."""
    session = conn.session
    host_url = normalize_host_with_protocol(session.host)
    url = f"{host_url}/telemetry-ext"

    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    session.auth_provider.add_headers(headers)
    headers.update(session.get_spog_headers())

    # Send a minimal valid telemetry payload
    payload = json.dumps({"uploadTime": int(time.time() * 1000), "items": [], "protoLogs": []})
    response = session.http_client.request(HttpMethod.POST, url, headers=headers, body=payload, timeout=30)
    assert response.status == 200, f"Telemetry {label} returned {response.status}, expected 200"


def verify_feature_flags(conn, label=""):
    """Directly hit the feature flags endpoint using the connection's auth and assert 200."""
    from databricks.sql import __version__
    session = conn.session
    host_url = normalize_host_with_protocol(session.host)
    url = f"{host_url}/api/2.0/connector-service/feature-flags/PYTHON/{__version__}"

    headers = {}
    session.auth_provider.add_headers(headers)
    headers["User-Agent"] = session.useragent_header
    headers.update(session.get_spog_headers())

    response = session.http_client.request(HttpMethod.GET, url, headers=headers, timeout=30)
    assert response.status == 200, f"Feature flags {label} returned {response.status}, expected 200"


def test_with_endpoints(name, host, http_path, token=None, credentials_provider=None,
                        use_sea=False):
    """Test data path + explicitly assert telemetry and feature flags return 200."""
    global passed, failed
    print(f"{name:<60}", end="", flush=True)
    t = time.time()
    try:
        kwargs = {"server_hostname": host, "http_path": http_path}
        if token:
            kwargs["access_token"] = token
        if credentials_provider:
            kwargs["credentials_provider"] = credentials_provider
        if use_sea:
            kwargs["use_sea"] = True

        conn = sql.connect(**kwargs)

        # 1. Verify data path
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS v")
        row = cursor.fetchone()
        assert row[0] == 1, f"Expected 1, got {row[0]}"
        cursor.close()

        # 2. Verify telemetry endpoint returns 200
        verify_telemetry(conn, label=name)

        # 3. Verify feature flags endpoint returns 200
        verify_feature_flags(conn, label=name)

        conn.close()
        print(f"PASS ({time.time() - t:.1f}s)")
        passed += 1
    except Exception as e:
        print(f"FAIL ({time.time() - t:.1f}s)")
        print(f"  {type(e).__name__}: {str(e)[:300]}")
        failed += 1


def m2m_cred_provider(host):
    from databricks.sdk.core import oauth_service_principal, Config
    def provider():
        config = Config(
            host=f"https://{host}",
            client_id=M2M_CLIENT_ID,
            client_secret=M2M_CLIENT_SECRET,
        )
        return oauth_service_principal(config)
    return provider


def main():
    if not PAT:
        print("ERROR: Set DATABRICKS_SPOG_TOKEN")
        sys.exit(1)

    print("=== Python SQL Connector SPOG Tests ===\n")

    # --- DBSQL + PAT + Thrift ---
    test("Legacy + Thrift (PAT)", LEGACY_HOST, WAREHOUSE_HTTP_PATH, token=PAT)
    test("SPOG + ?o= + Thrift (PAT)", SPOG_HOST, WAREHOUSE_HTTP_PATH_SPOG, token=PAT)
    test("SPOG no routing + Thrift (expect fail)", SPOG_HOST, WAREHOUSE_HTTP_PATH,
         token=PAT, expect_fail=True)
    test("Legacy + ?o= + Thrift (PAT)", LEGACY_HOST, WAREHOUSE_HTTP_PATH_SPOG, token=PAT)

    # --- DBSQL + PAT + SEA ---
    test("Legacy + SEA (PAT)", LEGACY_HOST, WAREHOUSE_HTTP_PATH, token=PAT, use_sea=True)
    test("SPOG + ?o= + SEA (PAT)", SPOG_HOST, WAREHOUSE_HTTP_PATH_SPOG, token=PAT, use_sea=True)
    test("SPOG no routing + SEA (expect fail)", SPOG_HOST, WAREHOUSE_HTTP_PATH,
         token=PAT, use_sea=True, expect_fail=True)
    test("Legacy + ?o= + SEA (PAT)", LEGACY_HOST, WAREHOUSE_HTTP_PATH_SPOG,
         token=PAT, use_sea=True)

    # --- GP Cluster ---
    if GP_CLUSTER_PATH:
        test("SPOG GP Cluster (PAT, Thrift)", SPOG_HOST, GP_CLUSTER_PATH, token=PAT)
        test("Legacy GP Cluster (PAT, Thrift)", LEGACY_HOST, GP_CLUSTER_PATH, token=PAT)
    else:
        skip("SPOG GP Cluster", "DATABRICKS_SPOG_GP_CLUSTER_PATH")
        skip("Legacy GP Cluster", "DATABRICKS_SPOG_GP_CLUSTER_PATH")

    # --- OAuth M2M ---
    try:
        from databricks.sdk.core import oauth_service_principal, Config
        has_sdk = True
    except ImportError:
        has_sdk = False

    if M2M_CLIENT_ID and M2M_CLIENT_SECRET and has_sdk:
        test("SPOG + ?o= + M2M (Thrift)", SPOG_HOST, WAREHOUSE_HTTP_PATH_SPOG,
             credentials_provider=m2m_cred_provider(SPOG_HOST))
        test("SPOG + ?o= + M2M (SEA)", SPOG_HOST, WAREHOUSE_HTTP_PATH_SPOG,
             credentials_provider=m2m_cred_provider(SPOG_HOST), use_sea=True)
        test("Legacy + M2M (Thrift)", LEGACY_HOST, WAREHOUSE_HTTP_PATH,
             credentials_provider=m2m_cred_provider(LEGACY_HOST))
    elif not has_sdk:
        skip("SPOG M2M (Thrift)", "databricks-sdk not installed")
        skip("SPOG M2M (SEA)", "databricks-sdk not installed")
        skip("Legacy M2M", "databricks-sdk not installed")
    else:
        skip("SPOG M2M (Thrift)", "M2M creds")
        skip("SPOG M2M (SEA)", "M2M creds")
        skip("Legacy M2M", "M2M creds")

    # --- Telemetry + Feature Flags (PAT, explicit endpoint verification) ---
    test_with_endpoints("SPOG + Telemetry + FF (PAT, Thrift)", SPOG_HOST,
                        WAREHOUSE_HTTP_PATH_SPOG, token=PAT)
    test_with_endpoints("SPOG + Telemetry + FF (PAT, SEA)", SPOG_HOST,
                        WAREHOUSE_HTTP_PATH_SPOG, token=PAT, use_sea=True)

    # --- OAuth M2M with Telemetry + Feature Flags ---
    if M2M_CLIENT_ID and M2M_CLIENT_SECRET and has_sdk:
        test_with_endpoints("SPOG + Telemetry + FF (M2M, Thrift)", SPOG_HOST,
                            WAREHOUSE_HTTP_PATH_SPOG,
                            credentials_provider=m2m_cred_provider(SPOG_HOST))
        test_with_endpoints("SPOG + Telemetry + FF (M2M, SEA)", SPOG_HOST,
                            WAREHOUSE_HTTP_PATH_SPOG,
                            credentials_provider=m2m_cred_provider(SPOG_HOST), use_sea=True)
    else:
        skip("SPOG + Telemetry + FF (M2M, Thrift)", "M2M creds or SDK")
        skip("SPOG + Telemetry + FF (M2M, SEA)", "M2M creds or SDK")

    # --- OAuth U2M with Telemetry + Feature Flags (browser login) ---
    if os.getenv("DATABRICKS_SPOG_U2M_ENABLED") == "true":
        test_with_endpoints("SPOG + Telemetry + FF (U2M, Thrift)", SPOG_HOST,
                            WAREHOUSE_HTTP_PATH_SPOG)
        test_with_endpoints("SPOG + Telemetry + FF (U2M, SEA)", SPOG_HOST,
                            WAREHOUSE_HTTP_PATH_SPOG, use_sea=True)
        test("Legacy + U2M (Thrift)", LEGACY_HOST, WAREHOUSE_HTTP_PATH)
    else:
        skip("SPOG + Telemetry + FF (U2M, Thrift)", "DATABRICKS_SPOG_U2M_ENABLED")
        skip("SPOG + Telemetry + FF (U2M, SEA)", "DATABRICKS_SPOG_U2M_ENABLED")
        skip("Legacy U2M (Thrift)", "DATABRICKS_SPOG_U2M_ENABLED")

    # --- Volume LIST ---
    def volume_list(host, path, use_sea=False):
        def _test():
            kwargs = {"server_hostname": host, "http_path": path, "access_token": PAT}
            if use_sea:
                kwargs["use_sea"] = True
            conn = sql.connect(**kwargs)
            cursor = conn.cursor()
            cursor.execute("LIST '/Volumes/main/default/_'")
            rows = cursor.fetchall()
            assert len(rows) > 0, f"Expected rows, got {len(rows)}"
            cursor.close()
            conn.close()
        return _test

    test_custom("SPOG Volume LIST (Thrift)", volume_list(SPOG_HOST, WAREHOUSE_HTTP_PATH_SPOG))
    test_custom("Legacy Volume LIST (Thrift)", volume_list(LEGACY_HOST, WAREHOUSE_HTTP_PATH))

    print(f"\n=== Results: {passed} passed, {failed} failed, {skipped} skipped ===")


if __name__ == "__main__":
    main()
