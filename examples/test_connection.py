#!/usr/bin/env python3
"""Quick smoke test: connect and run one CREATE TABLE."""

import logging
import time
from databricks import sql
from load_credentials import load_credentials

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")

_creds = load_credentials()
SERVER_HOSTNAME = _creds["SERVER_HOSTNAME"]
HTTP_PATH = _creds["HTTP_PATH"]
ACCESS_TOKEN = _creds["ACCESS_TOKEN"]
CATALOG = _creds["CATALOG"]
SCHEMA = _creds["SCHEMA"]

print("Connecting...")
t0 = time.time()

with sql.connect(
    server_hostname=SERVER_HOSTNAME,
    http_path=HTTP_PATH,
    access_token=ACCESS_TOKEN,
    _tls_no_verify=True,
    enable_telemetry=False,
) as conn:
    print(f"Connected in {time.time() - t0:.2f}s")

    with conn.cursor() as cursor:
        cursor.execute(f"USE CATALOG {CATALOG}")
        print(f"USE CATALOG done in {time.time() - t0:.2f}s")

        cursor.execute(f"USE SCHEMA {SCHEMA}")
        print(f"USE SCHEMA done in {time.time() - t0:.2f}s")

        t1 = time.time()
        cursor.execute("CREATE TABLE IF NOT EXISTS test_conn_check (col1 STRING, col2 STRING)")
        print(f"CREATE TABLE done in {time.time() - t1:.2f}s")

        t1 = time.time()
        cursor.execute("SELECT 1")
        print(f"SELECT 1 done in {time.time() - t1:.2f}s")

        t1 = time.time()
        cursor.execute("DROP TABLE IF EXISTS test_conn_check")
        print(f"DROP TABLE done in {time.time() - t1:.2f}s")

print(f"Total: {time.time() - t0:.2f}s")