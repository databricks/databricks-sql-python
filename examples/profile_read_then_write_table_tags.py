#!/usr/bin/env python3
"""
Profile information_schema.table_tags SELECT performance.

For each table, this script SELECTs existing table tags from
system.information_schema.table_tags. No ALTER/write operations.

Usage:
    python examples/profile_read_then_write_table_tags.py --threads 1 --iterations 1 --validate
    python examples/profile_read_then_write_table_tags.py --threads 8 --iterations 10
"""

import argparse
import json
import logging
import os
import random
import re
import statistics
import string
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Empty, Queue

sys.stdout.reconfigure(line_buffering=True)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from databricks import sql

# ============================================================
# CONFIGURATION — loaded from examples/credentials.env
# ============================================================
from load_credentials import load_credentials
_creds = load_credentials()
SERVER_HOSTNAME = _creds["SERVER_HOSTNAME"]
HTTP_PATH = _creds["HTTP_PATH"]
ACCESS_TOKEN = _creds["ACCESS_TOKEN"]
CATALOG = _creds["CATALOG"]
SCHEMA = _creds["SCHEMA"]
# ============================================================

NUM_TABLES = 128
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "read_then_write_table_tags")

SELECT_TEMPLATE = """SELECT tag_name, tag_value
FROM system.information_schema.table_tags
WHERE catalog_name = '{catalog}'
  AND schema_name = '{schema}'
  AND table_name = '{table}'"""


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

class ProfileLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list = []

    def emit(self, record):
        msg = record.getMessage()
        if "[PROFILE]" in msg:
            self.records.append(
                {"timestamp": record.created, "thread": record.threadName, "message": msg}
            )


def setup_logging(log_path: str) -> ProfileLogHandler:
    profile_handler = ProfileLogHandler()
    profile_handler.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(threadName)s %(name)s %(levelname)s %(message)s")
    )

    for logger_name in [
        "databricks.sql.backend.thrift_backend",
        "databricks.sql.auth.retry",
        "databricks.sql.client",
    ]:
        lgr = logging.getLogger(logger_name)
        lgr.setLevel(logging.DEBUG)
        lgr.addHandler(profile_handler)
        lgr.addHandler(file_handler)

    return profile_handler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def conn_params() -> dict:
    return {
        "server_hostname": SERVER_HOSTNAME,
        "http_path": HTTP_PATH,
        "access_token": ACCESS_TOKEN,
        "_tls_no_verify": True,
        "enable_telemetry": False,
    }


def random_tag_value(length: int = 5) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def percentile(data: list, p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def latency_stats(latencies: list) -> dict:
    if not latencies:
        return {k: 0.0 for k in ["count", "min", "max", "mean", "stdev", "p50", "p90", "p95", "p99"]}
    return {
        "count": len(latencies),
        "min": min(latencies),
        "max": max(latencies),
        "mean": statistics.mean(latencies),
        "stdev": statistics.stdev(latencies) if len(latencies) > 1 else 0.0,
        "p50": percentile(latencies, 50),
        "p90": percentile(latencies, 90),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
    }


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def worker(
    thread_id: int,
    table_queue: Queue,
    results: list,
    results_lock: threading.Lock,
):
    local_results = []
    table_fqn_prefix = f"`{CATALOG}`.`{SCHEMA}`"

    with sql.connect(**conn_params()) as connection:
        with connection.cursor() as cursor:
            # Warmup
            cursor.execute("SELECT 1")

            while True:
                try:
                    table_name = table_queue.get_nowait()
                except Empty:
                    break

                table_fqn = f"{table_fqn_prefix}.{table_name}"

                # --- Step 1: Read table tags from information_schema ---
                select_sql = SELECT_TEMPLATE.format(
                    catalog=CATALOG, schema=SCHEMA, table=table_name
                )

                op_start = time.perf_counter()
                select_start = time.perf_counter()
                select_success = True
                select_error_type = None
                select_error_message = None
                select_rows = 0
                select_statement_id = None

                try:
                    cursor.execute(select_sql)
                    select_statement_id = str(cursor.active_command_id) if cursor.active_command_id else None
                    rows = cursor.fetchall()
                    select_rows = len(rows)
                except Exception as e:
                    select_success = False
                    select_error_type = type(e).__name__
                    select_error_message = str(e)[:500]

                select_end = time.perf_counter()
                select_latency_ms = (select_end - select_start) * 1000

                local_results.append(
                    {
                        "table": table_name,
                        "thread_id": thread_id,
                        "select_latency_ms": round(select_latency_ms, 2),
                        "select_success": select_success,
                        "select_error_type": select_error_type,
                        "select_error_message": select_error_message,
                        "select_rows": select_rows,
                        "select_statement_id": select_statement_id,
                        "timestamp": op_start,
                    }
                )

    with results_lock:
        results.extend(local_results)


# ---------------------------------------------------------------------------
# Run one iteration
# ---------------------------------------------------------------------------

def run_iteration(iteration: int, num_threads: int, tables_per_iteration: int) -> tuple:
    table_queue = Queue()
    start = ((iteration - 1) * tables_per_iteration) % NUM_TABLES
    for i in range(tables_per_iteration):
        table_idx = start + i + 1
        table_queue.put(f"table{table_idx}")

    results: list = []
    results_lock = threading.Lock()

    iter_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(worker, tid, table_queue, results, results_lock)
            for tid in range(num_threads)
        ]
        for f in as_completed(futures):
            f.result()

    iter_end = time.perf_counter()
    duration_s = iter_end - iter_start

    for r in results:
        r["iteration"] = iteration

    return results, duration_s


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(
    args,
    all_results: list,
    iteration_durations: list,
    profile_handler: ProfileLogHandler,
    report_path: str,
):
    lines = []

    def w(text=""):
        lines.append(text)

    total_ops = len(all_results)
    total_duration = sum(iteration_durations)

    select_ok = [r for r in all_results if r["select_success"]]
    select_fail = [r for r in all_results if not r["select_success"]]
    select_latencies = [r["select_latency_ms"] for r in select_ok]

    # --- Header ---
    w(f"# Information Schema Table Tags Profile: N={args.threads}, I={args.iterations}")
    w()
    w("## Configuration")
    w(f"- **Server**: `{SERVER_HOSTNAME}`")
    w(f"- **HTTP Path**: `{HTTP_PATH}`")
    w(f"- **Catalog.Schema**: `{CATALOG}.{SCHEMA}`")
    w(f"- **Tables**: {NUM_TABLES}")
    w(f"- **Pattern**: SELECT from system.information_schema.table_tags per table")
    w(f"- **Threads**: {args.threads}")
    w(f"- **Iterations**: {args.iterations}")
    w(f"- **Total SELECTs**: {total_ops}")
    w(f"- **Date**: {datetime.now().isoformat()}")
    w()

    # --- Latency ---
    w("## SELECT Latency (ms)")
    w()
    ss = latency_stats(select_latencies)
    w("| Metric | Value |")
    w("|--------|-------|")
    for k in ["count", "min", "max", "mean", "stdev", "p50", "p90", "p95", "p99"]:
        w(f"| {k} | {ss[k]:.2f} |")
    w()

    # --- Throughput ---
    w("## Throughput")
    w()
    w(f"- **Total SELECTs**: {total_ops}")
    w(f"- **Successes**: {len(select_ok)} / {total_ops}")
    w(f"- **Failures**: {len(select_fail)} / {total_ops}")
    w(f"- **Total wall-clock**: {total_duration:.2f}s")
    if total_duration > 0:
        w(f"- **SELECTs/sec**: {total_ops / total_duration:.2f}")
    w()

    # --- Cold Start vs Steady State ---
    if args.iterations > 1:
        w("## Cold Start vs Steady State")
        w()
        iter1 = [r["select_latency_ms"] for r in select_ok if r["iteration"] == 1]
        iter_rest = [r["select_latency_ms"] for r in select_ok if r["iteration"] > 1]
        s1 = latency_stats(iter1)
        sr = latency_stats(iter_rest)
        w("| Phase | Count | P50 (ms) | P90 (ms) | P99 (ms) |")
        w("|-------|-------|----------|----------|----------|")
        w(f"| Iteration 1 | {s1['count']:.0f} | {s1['p50']:.2f} | {s1['p90']:.2f} | {s1['p99']:.2f} |")
        w(f"| Iterations 2-{args.iterations} | {sr['count']:.0f} | {sr['p50']:.2f} | {sr['p90']:.2f} | {sr['p99']:.2f} |")
        w()

    # --- Per-Iteration Summary ---
    w("## Per-Iteration Summary")
    w()
    w("| Iteration | SELECTs | P50 (ms) | P90 (ms) | P99 (ms) | Errors | Duration (s) | SELECTs/sec |")
    w("|-----------|---------|----------|----------|----------|--------|--------------|-------------|")
    for i in range(1, args.iterations + 1):
        i_lats = [r["select_latency_ms"] for r in select_ok if r["iteration"] == i]
        i_errs = len([r for r in all_results if r["iteration"] == i and not r["select_success"]])
        dur = iteration_durations[i - 1]
        ops_in_iter = len([r for r in all_results if r["iteration"] == i])
        rps = ops_in_iter / dur if dur > 0 else 0
        s = latency_stats(i_lats)
        w(f"| {i} | {s['count']:.0f} | {s['p50']:.2f} | {s['p90']:.2f} | {s['p99']:.2f} | {i_errs} | {dur:.2f} | {rps:.2f} |")
    w()

    # --- All SELECTs with Statement IDs ---
    w("## All SELECTs with Statement IDs")
    w()
    w("| Table | Iteration | Latency (ms) | Rows | Statement ID |")
    w("|-------|-----------|-------------|------|--------------|")
    sorted_results = sorted(all_results, key=lambda r: (r.get("iteration", 0), int(r["table"].replace("table", ""))))
    for r in sorted_results:
        w(
            f"| {r['table']} | {r.get('iteration', '?')} "
            f"| {r['select_latency_ms']:.2f} | {r['select_rows']} "
            f"| {r.get('select_statement_id', 'N/A')} |"
        )
    w()

    # --- By Thread ---
    w("## Latency by Thread (ms)")
    w()
    w("| Thread | SELECTs | P50 | P90 | P99 |")
    w("|--------|---------|-----|-----|-----|")
    threads_seen = sorted(set(r["thread_id"] for r in select_ok))
    for tid in threads_seen:
        t_lats = latency_stats([r["select_latency_ms"] for r in select_ok if r["thread_id"] == tid])
        w(f"| {tid} | {t_lats['count']:.0f} | {t_lats['p50']:.2f} | {t_lats['p90']:.2f} | {t_lats['p99']:.2f} |")
    w()

    # --- Rows returned by SELECT ---
    w("## Information Schema Rows Returned")
    w()
    row_counts = [r["select_rows"] for r in select_ok]
    if row_counts:
        w(f"- **Min rows**: {min(row_counts)}")
        w(f"- **Max rows**: {max(row_counts)}")
        w(f"- **Mean rows**: {statistics.mean(row_counts):.1f}")
    w()

    # --- Error Analysis ---
    w("## Error Analysis")
    w()
    all_errors = []
    for r in all_results:
        if not r["select_success"]:
            all_errors.append({"table": r["table"], "iteration": r.get("iteration", "?"),
                               "error_type": r["select_error_type"], "error_message": r["select_error_message"]})

    if not all_errors:
        w("No errors encountered.")
    else:
        error_groups = defaultdict(list)
        for e in all_errors:
            error_groups[e["error_type"]].append(e)
        w("| Error Type | Count | % of Total | Sample Message |")
        w("|------------|-------|------------|----------------|")
        for etype, records in sorted(error_groups.items(), key=lambda x: -len(x[1])):
            pct = len(records) / total_ops * 100
            sample = records[0]["error_message"][:200] if records[0]["error_message"] else "N/A"
            w(f"| {etype} | {len(records)} | {pct:.1f}% | {sample} |")
        w()

        w("### Error Detail")
        w()
        for etype, records in sorted(error_groups.items(), key=lambda x: -len(x[1])):
            w(f"**{etype}** ({len(records)} occurrences)")
            w()
            for e in records[:3]:
                w(f"- Table: {e['table']}, Iteration: {e['iteration']}")
                w(f"  Message: {e['error_message']}")
            if len(records) > 3:
                w(f"- ... and {len(records) - 3} more")
            w()
    w()

    # --- Retry Analysis ---
    ATTEMPT_RE = re.compile(r"\[PROFILE\] (?P<cmd>\w+) attempt (?P<attempt>\d+)/(?P<max>\d+)")
    SUCCESS_RE = re.compile(r"\[PROFILE\] (?P<cmd>\w+) succeeded on attempt (?P<attempt>\d+) in (?P<elapsed>[0-9.]+)s")
    SHOULD_RETRY_RE = re.compile(r"\[PROFILE\] should_retry: status=(?P<status>\d+), command=(?P<cmd>[^,]+),")
    RETRY_SLEEP_RE = re.compile(r"\[PROFILE\] (?P<cmd>\w+) retry sleep=(?P<sleep>[0-9.]+)s, attempt=(?P<attempt>\d+)/(?P<max>\d+)")

    parsed_events = []
    for r in profile_handler.records:
        msg = r["message"]
        for etype, regex in [("attempt", ATTEMPT_RE), ("success", SUCCESS_RE),
                             ("should_retry", SHOULD_RETRY_RE), ("retry_sleep", RETRY_SLEEP_RE)]:
            m = regex.search(msg)
            if m:
                event = {"type": etype, "thread": r["thread"], "timestamp": r["timestamp"], "message": msg}
                event.update(m.groupdict())
                if "attempt" in event:
                    event["attempt"] = int(event["attempt"])
                if "status" in event:
                    event["status"] = int(event["status"])
                parsed_events.append(event)
                break

    exec_events = [e for e in parsed_events if e.get("cmd") == "ExecuteStatement"]
    exec_retry_sleeps = [e for e in exec_events if e["type"] == "retry_sleep"]
    exec_should_retry = [e for e in exec_events if e["type"] == "should_retry"]
    exec_success_after_retry = [e for e in exec_events if e["type"] == "success" and e["attempt"] > 1]
    exec_total_attempts = [e for e in exec_events if e["type"] == "attempt"]
    exec_successes = [e for e in exec_events if e["type"] == "success"]

    w("## Statement Retry Analysis (ExecuteStatement only)")
    w()
    w("*Includes information_schema SELECTs and one warmup SELECT 1 per thread.*")
    w()
    w(f"- **Total [PROFILE] events (all commands)**: {len(parsed_events)}")
    w(f"- **ExecuteStatement attempts**: {len(exec_total_attempts)}")
    w(f"- **ExecuteStatement successes**: {len(exec_successes)}")
    w(f"- **ExecuteStatement retry sleeps**: {len(exec_retry_sleeps)}")
    w(f"- **ExecuteStatement succeeded after retry (attempt > 1)**: {len(exec_success_after_retry)}")
    w(f"- **should_retry evaluations**: {len(exec_should_retry)}")
    w()

    if exec_retry_sleeps:
        w("### Retry Events")
        w()
        w("| Timestamp | Thread | Attempt | Sleep (s) | Message |")
        w("|-----------|--------|---------|-----------|---------|")
        for e in exec_retry_sleeps[:50]:
            ts = datetime.fromtimestamp(e["timestamp"]).strftime("%H:%M:%S.%f")[:-3]
            w(f"| {ts} | {e['thread']} | {e['attempt']} | {e.get('sleep', '?')} | {e['message'][:150]} |")
        if len(exec_retry_sleeps) > 50:
            w(f"| ... | ... | ... | ... | {len(exec_retry_sleeps) - 50} more |")
        w()

    if exec_should_retry:
        w("### should_retry Decisions")
        w()
        status_counts = defaultdict(int)
        for e in exec_should_retry:
            status_counts[e["status"]] += 1
        w("| HTTP Status | Count |")
        w("|-------------|-------|")
        for status, count in sorted(status_counts.items()):
            w(f"| {status} | {count} |")
        w()

    # --- Footer ---
    w("---")
    w(f"*Generated by profile_read_then_write_table_tags.py on {datetime.now().isoformat()}*")

    report_text = "\n".join(lines)
    with open(report_path, "w") as f:
        f.write(report_text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Profile read-from-information_schema then write-table-tag pattern"
    )
    parser.add_argument("--threads", type=int, required=True, help="Number of concurrent threads")
    parser.add_argument("--iterations", type=int, required=True, help="Number of iterations")
    parser.add_argument("--tables-per-iteration", type=int, default=None, help="Tables per iteration (default = --threads)")
    parser.add_argument("--validate", action="store_true", help="Quick validation: override to 1 iteration")
    args = parser.parse_args()

    if args.tables_per_iteration is None:
        args.tables_per_iteration = args.threads

    if args.tables_per_iteration > NUM_TABLES:
        print(f"Error: --tables-per-iteration {args.tables_per_iteration} exceeds NUM_TABLES={NUM_TABLES}")
        sys.exit(1)

    if args.validate:
        args.iterations = 1
        print("=== VALIDATION MODE: 1 iteration only ===\n")

    os.makedirs(RESULTS_DIR, exist_ok=True)
    prefix = f"rwtt_n{args.threads}_i{args.iterations}"
    report_path = os.path.join(RESULTS_DIR, f"{prefix}_report.md")
    data_path = os.path.join(RESULTS_DIR, f"{prefix}_data.jsonl")
    log_path = os.path.join(RESULTS_DIR, f"{prefix}_retries.log")

    profile_handler = setup_logging(log_path)

    print(f"Profile (information_schema.table_tags): threads={args.threads}, iterations={args.iterations}, tables_per_iteration={args.tables_per_iteration}")
    print(f"SELECTs per iteration: {args.tables_per_iteration} (1 per table)")
    print(f"Total SELECTs: {args.tables_per_iteration * args.iterations}")
    print(f"Output: {report_path}")
    print()

    all_results = []
    iteration_durations = []

    for i in range(1, args.iterations + 1):
        print(f"Iteration {i}/{args.iterations}...", end=" ", flush=True)
        results, duration = run_iteration(iteration=i, num_threads=args.threads, tables_per_iteration=args.tables_per_iteration)
        all_results.extend(results)
        iteration_durations.append(duration)

        errs = len([r for r in results if not r["select_success"]])
        rps = len(results) / duration if duration > 0 else 0
        print(f"done in {duration:.2f}s ({len(results)} SELECTs, {errs} errors, {rps:.1f} SELECTs/sec)")

    print()

    with open(data_path, "w") as f:
        for r in all_results:
            f.write(json.dumps(r) + "\n")

    generate_report(args, all_results, iteration_durations, profile_handler, report_path)

    print(f"Report written to: {report_path}")
    print(f"Raw data written to: {data_path}")
    print(f"Retry log written to: {log_path}")

    ok = [r for r in all_results if r["select_success"]]
    if ok:
        s = latency_stats([r["select_latency_ms"] for r in ok])
        total_dur = sum(iteration_durations)
        print()
        print("=== Summary ===")
        print(f"  SELECTs: {len(all_results)} ({len(ok)} ok, {len(all_results) - len(ok)} failed)")
        print(f"  Latency: p50={s['p50']:.1f}ms  p90={s['p90']:.1f}ms  p99={s['p99']:.1f}ms  max={s['max']:.1f}ms")
        print(f"  Throughput: {len(all_results) / total_dur:.1f} SELECTs/sec")


if __name__ == "__main__":
    main()