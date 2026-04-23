#!/usr/bin/env python3
"""Remove all column tags and table tags from all 64 tables using 32 threads."""

import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(line_buffering=True)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from databricks import sql
from load_credentials import load_credentials

_creds = load_credentials()
SERVER_HOSTNAME = _creds["SERVER_HOSTNAME"]
HTTP_PATH = _creds["HTTP_PATH"]
ACCESS_TOKEN = _creds["ACCESS_TOKEN"]
CATALOG = _creds["CATALOG"]
SCHEMA = _creds["SCHEMA"]

NUM_TABLES = 64
NUM_THREADS = 32


def cleanup_table(table_name):
    table_fqn = f"`{CATALOG}`.`{SCHEMA}`.{table_name}"
    total_removed = 0

    with sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
        _tls_no_verify=True,
    ) as conn:
        with conn.cursor() as cursor:
            # --- Clean up column tags ---
            cursor.execute(
                f"SELECT column_name, tag_name FROM system.information_schema.column_tags "
                f"WHERE catalog_name = '{CATALOG}' AND schema_name = '{SCHEMA}' AND table_name = '{table_name}'"
            )
            col_rows = cursor.fetchall()

            if col_rows:
                col_tags = defaultdict(list)
                for row in col_rows:
                    col_tags[row[0]].append(row[1])

                for col, tags in col_tags.items():
                    tag_list = ", ".join(f"'{tag}'" for tag in tags)
                    cursor.execute(f"ALTER TABLE {table_fqn} ALTER COLUMN {col} UNSET TAGS ({tag_list})")

                total_removed += len(col_rows)

            # --- Clean up table tags ---
            cursor.execute(
                f"SELECT tag_name FROM system.information_schema.table_tags "
                f"WHERE catalog_name = '{CATALOG}' AND schema_name = '{SCHEMA}' AND table_name = '{table_name}'"
            )
            tbl_rows = cursor.fetchall()

            if tbl_rows:
                tag_list = ", ".join(f"'{row[0]}'" for row in tbl_rows)
                cursor.execute(f"ALTER TABLE {table_fqn} UNSET TAGS ({tag_list})")
                total_removed += len(tbl_rows)

            print(f"{table_name}: removed {len(col_rows)} column tags, {len(tbl_rows)} table tags")
            return total_removed


total_removed = 0
with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    futures = {
        executor.submit(cleanup_table, f"table{t}"): t
        for t in range(1, NUM_TABLES + 1)
    }
    for f in as_completed(futures):
        total_removed += f.result()

print(f"\nDone. Removed {total_removed} total tags (column + table).")
