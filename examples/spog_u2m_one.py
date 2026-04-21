"""Minimal pysql U2M (databricks-oauth) test — one host per invocation.

Usage: .venv/bin/python examples/spog_u2m_one.py <spog|legacy>
"""
import os, sys, time
from databricks import sql

TARGET = sys.argv[1] if len(sys.argv) > 1 else "spog"
if TARGET == "spog":
    HOST = "dogfood-spog.staging.azuredatabricks.net"
    PATH = "/sql/1.0/warehouses/e256699345d1ac74?o=7064161269814046"
elif TARGET == "prod-legacy":
    HOST = "adb-6436897454825492.12.azuredatabricks.net"
    PATH = "/sql/1.0/warehouses/00adc7b6c00429b8"
else:
    HOST = "adb-7064161269814046.2.staging.azuredatabricks.net"
    PATH = "/sql/1.0/warehouses/e256699345d1ac74"

print(f"Python U2M ({TARGET}) — browser will open, complete login.")
t0 = time.time()
try:
    with sql.connect(
        server_hostname=HOST,
        http_path=PATH,
        auth_type="databricks-oauth",
    ) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 12345678 AS v")
        row = cur.fetchone()
        cur.close()
    if row and row[0] == 12345678:
        print(f"U2M {TARGET}: PASS ({time.time()-t0:.1f}s)")
    else:
        print(f"U2M {TARGET}: FAIL — unexpected {row!r}")
except Exception as e:
    print(f"U2M {TARGET}: FAIL — {type(e).__name__}: {str(e)[:300]}")
