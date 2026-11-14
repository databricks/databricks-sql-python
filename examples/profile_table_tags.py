#!/usr/bin/env python3
"""
Profile SET TABLE TAGS performance on Databricks.

Uses existing tables (table1..table64). No column tags — only table-level tags.

Usage:
    # Quick validation
    python examples/profile_table_tags.py --tags 1 --threads 1 --iterations 1 --validate

    # Single experiment
    python examples/profile_table_tags.py --tags 4 --threads 8 --iterations 10

    # Full sweep
    for t in 1 2 4; do
      for n in 1 2 4 8 16; do
        python examples/profile_table_tags.py --tags $t --threads $n --iterations 10
      done
    done
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

# Force unbuffered stdout so output is visible when piped through grep
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
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "table_tags")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

class ProfileLogHandler(logging.Handler):
    """Captures [PROFILE] log lines for retry analysis."""

    def __init__(self):
        super().__init__()
        self.records: list = []

    def emit(self, record):
        msg = record.getMessage()
        if "[PROFILE]" in msg:
            self.records.append(
                {
                    "timestamp": record.created,
                    "thread": record.threadName,
                    "message": msg,
                }
            )


def setup_logging(log_path: str) -> ProfileLogHandler:
    """Configure logging: file handler for all connector logs, profile handler for [PROFILE] lines."""
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


def build_table_tag_sql(table_fqn: str, num_tags: int) -> str:
    tags = ", ".join(f"'key{i}' = '{random_tag_value()}'" for i in range(1, num_tags + 1))
    return f"ALTER TABLE {table_fqn} SET TAGS ({tags})"


def percentile(data: list, p: float) -> float:
    """Return the p-th percentile (0-100) of data."""
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
    """Compute full latency statistics for a list of ms values."""
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
    num_tags: int,
    alter_results: list,
    results_lock: threading.Lock,
):
    """Worker thread: pulls tables from queue, sets table-level tags, records metrics."""
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
                alter_sql = build_table_tag_sql(table_fqn, num_tags)

                cmd_start = time.perf_counter()
                success = True
                error_type = None
                error_message = None
                error_context = None

                try:
                    cursor.execute(alter_sql)
                except Exception as e:
                    success = False
                    error_type = type(e).__name__
                    error_message = str(e)[:500]
                    error_context = getattr(e, "context", None)

                cmd_end = time.perf_counter()
                latency_ms = (cmd_end - cmd_start) * 1000

                local_results.append(
                    {
                        "table": table_name,
                        "thread_id": thread_id,
                        "latency_ms": round(latency_ms, 2),
                        "success": success,
                        "error_type": error_type,
                        "error_message": error_message,
                        "error_context": str(error_context) if error_context else None,
                        "timestamp": cmd_start,
                    }
                )

    with results_lock:
        alter_results.extend(local_results)


# ---------------------------------------------------------------------------
# Run one iteration
# ---------------------------------------------------------------------------

def run_iteration(
    iteration: int,
    num_tags: int,
    num_threads: int,
    tables_per_iteration: int,
) -> tuple:
    """Run a single iteration: tables_per_iteration tables distributed across threads."""
    table_queue = Queue()
    start = ((iteration - 1) * tables_per_iteration) % NUM_TABLES
    for i in range(tables_per_iteration):
        table_idx = start + i + 1
        table_queue.put(f"table{table_idx}")

    alter_results: list = []
    results_lock = threading.Lock()

    iter_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for tid in range(num_threads):
            f = executor.submit(
                worker,
                tid,
                table_queue,
                num_tags,
                alter_results,
                results_lock,
            )
            futures.append(f)

        for f in as_completed(futures):
            f.result()  # raise any thread exceptions

    iter_end = time.perf_counter()
    duration_s = iter_end - iter_start

    for r in alter_results:
        r["iteration"] = iteration

    return alter_results, duration_s


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
    """Generate the markdown report."""
    lines = []

    def w(text=""):
        lines.append(text)

    total_alters = len(all_results)
    total_duration = sum(iteration_durations)
    successful = [r for r in all_results if r["success"]]
    failed = [r for r in all_results if not r["success"]]
    success_latencies = [r["latency_ms"] for r in successful]

    # --- Header ---
    w(f"# Table Tags Profile: T={args.tags}, N={args.threads}, I={args.iterations}")
    w()
    w("## Configuration")
    w(f"- **Server**: `{SERVER_HOSTNAME}`")
    w(f"- **HTTP Path**: `{HTTP_PATH}`")
    w(f"- **Catalog.Schema**: `{CATALOG}.{SCHEMA}`")
    w(f"- **Tables**: {NUM_TABLES}")
    w(f"- **Tags per ALTER**: {args.tags}")
    w(f"- **Threads**: {args.threads}")
    w(f"- **Iterations**: {args.iterations}")
    w(f"- **Total ALTERs**: {total_alters}")
    w(f"- **Date**: {datetime.now().isoformat()}")
    w()

    # --- Overall ALTER Latency ---
    w("## Per-ALTER Latency — All Iterations (ms)")
    w()
    stats = latency_stats(success_latencies)
    w("| Metric | Value |")
    w("|--------|-------|")
    for k, v in stats.items():
        w(f"| {k} | {v:.2f} |")
    w()

    # --- Throughput ---
    w("## Throughput")
    w()
    w(f"- **Total ALTERs**: {total_alters}")
    w(f"- **Successful**: {len(successful)}")
    w(f"- **Failed**: {len(failed)}")
    w(f"- **Total wall-clock**: {total_duration:.2f}s")
    if total_duration > 0:
        w(f"- **ALTERs/sec**: {total_alters / total_duration:.2f}")
    w()

    # --- Cold Start vs Steady State ---
    if args.iterations > 1:
        w("## Cold Start vs Steady State")
        w()
        iter1 = [r["latency_ms"] for r in successful if r["iteration"] == 1]
        iter_rest = [r["latency_ms"] for r in successful if r["iteration"] > 1]
        w("| Phase | ALTERs | Mean (ms) | P50 (ms) | P99 (ms) |")
        w("|-------|--------|-----------|----------|----------|")
        s1 = latency_stats(iter1)
        sr = latency_stats(iter_rest)
        w(f"| Iteration 1 | {s1['count']:.0f} | {s1['mean']:.2f} | {s1['p50']:.2f} | {s1['p99']:.2f} |")
        w(f"| Iterations 2-{args.iterations} | {sr['count']:.0f} | {sr['mean']:.2f} | {sr['p50']:.2f} | {sr['p99']:.2f} |")
        w()

    # --- Per-Iteration Summary ---
    w("## Per-Iteration Summary")
    w()
    w("| Iteration | ALTERs | Mean (ms) | P50 (ms) | P99 (ms) | Errors | Duration (s) | ALTERs/sec |")
    w("|-----------|--------|-----------|----------|----------|--------|--------------|------------|")
    for i in range(1, args.iterations + 1):
        iter_lats = [r["latency_ms"] for r in successful if r["iteration"] == i]
        iter_errs = len([r for r in failed if r["iteration"] == i])
        s = latency_stats(iter_lats)
        dur = iteration_durations[i - 1]
        alters_in_iter = len([r for r in all_results if r["iteration"] == i])
        rps = alters_in_iter / dur if dur > 0 else 0
        w(
            f"| {i} | {s['count']:.0f} | {s['mean']:.2f} | {s['p50']:.2f} | {s['p99']:.2f} "
            f"| {iter_errs} | {dur:.2f} | {rps:.2f} |"
        )
    w()

    # --- Per-ALTER Latency by Table ---
    w("## Per-ALTER Latency by Table (ms)")
    w()
    w("| Table | Count | Min | Max | Mean | P50 | P90 | P95 | P99 |")
    w("|-------|-------|-----|-----|------|-----|-----|-----|-----|")
    tables_seen = sorted(set(r["table"] for r in successful), key=lambda x: int(x.replace("table", "")))
    for tbl in tables_seen:
        tbl_lats = [r["latency_ms"] for r in successful if r["table"] == tbl]
        s = latency_stats(tbl_lats)
        w(
            f"| {tbl} | {s['count']:.0f} | {s['min']:.2f} | {s['max']:.2f} "
            f"| {s['mean']:.2f} | {s['p50']:.2f} | {s['p90']:.2f} | {s['p95']:.2f} | {s['p99']:.2f} |"
        )
    w()

    # --- Per-ALTER Latency by Thread ---
    w("## Per-ALTER Latency by Thread (ms)")
    w()
    w("| Thread | Count | Min | Max | Mean | P50 | P90 | P95 | P99 |")
    w("|--------|-------|-----|-----|------|-----|-----|-----|-----|")
    threads_seen = sorted(set(r["thread_id"] for r in successful))
    for tid in threads_seen:
        thr_lats = [r["latency_ms"] for r in successful if r["thread_id"] == tid]
        s = latency_stats(thr_lats)
        w(
            f"| {tid} | {s['count']:.0f} | {s['min']:.2f} | {s['max']:.2f} "
            f"| {s['mean']:.2f} | {s['p50']:.2f} | {s['p90']:.2f} | {s['p95']:.2f} | {s['p99']:.2f} |"
        )
    w()

    # --- Error Analysis ---
    w("## Error Analysis")
    w()
    if not failed:
        w("No errors encountered.")
    else:
        error_groups = defaultdict(list)
        for r in failed:
            error_groups[r["error_type"]].append(r)
        w("| Error Type | Count | % of Total | Sample Message |")
        w("|------------|-------|------------|----------------|")
        for etype, records in sorted(error_groups.items(), key=lambda x: -len(x[1])):
            pct = len(records) / total_alters * 100
            sample = records[0]["error_message"][:200] if records[0]["error_message"] else "N/A"
            w(f"| {etype} | {len(records)} | {pct:.1f}% | {sample} |")
        w()

        w("### Error Detail")
        w()
        for etype, records in sorted(error_groups.items(), key=lambda x: -len(x[1])):
            w(f"**{etype}** ({len(records)} occurrences)")
            w()
            for r in records[:3]:
                w(f"- Table: {r['table']}, Iteration: {r['iteration']}")
                w(f"  Latency: {r['latency_ms']:.2f}ms")
                w(f"  Message: {r['error_message']}")
                if r["error_context"]:
                    w(f"  Context: {r['error_context']}")
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
    w("*Includes benchmarked ALTERs + one warmup SELECT 1 per worker thread.*")
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
    w(f"*Generated by profile_table_tags.py on {datetime.now().isoformat()}*")

    report_text = "\n".join(lines)
    with open(report_path, "w") as f:
        f.write(report_text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Profile SET TABLE TAGS performance")
    parser.add_argument("--tags", type=int, required=True, help="Number of tags per ALTER command")
    parser.add_argument("--threads", type=int, required=True, help="Number of concurrent threads")
    parser.add_argument("--iterations", type=int, required=True, help="Number of iterations")
    parser.add_argument("--tables-per-iteration", type=int, default=None, help="Tables per iteration (default = --threads)")
    parser.add_argument("--validate", action="store_true", help="Quick validation: override to 1 iteration, print result")
    args = parser.parse_args()

    if args.tables_per_iteration is None:
        args.tables_per_iteration = args.threads

    if args.tables_per_iteration > NUM_TABLES:
        print(f"Error: --tables-per-iteration {args.tables_per_iteration} exceeds NUM_TABLES={NUM_TABLES}")
        sys.exit(1)

    if args.validate:
        args.iterations = 1
        print("=== VALIDATION MODE: 1 iteration only ===\n")

    # File paths
    os.makedirs(RESULTS_DIR, exist_ok=True)
    prefix = f"tt_t{args.tags}_n{args.threads}_i{args.iterations}"
    report_path = os.path.join(RESULTS_DIR, f"{prefix}_report.md")
    data_path = os.path.join(RESULTS_DIR, f"{prefix}_data.jsonl")
    log_path = os.path.join(RESULTS_DIR, f"{prefix}_retries.log")

    # Logging
    profile_handler = setup_logging(log_path)

    print(f"Profile (TABLE TAGS): tags={args.tags}, threads={args.threads}, iterations={args.iterations}, tables_per_iteration={args.tables_per_iteration}")
    print(f"ALTERs per iteration: {args.tables_per_iteration} (one per table)")
    print(f"Total ALTERs: {args.tables_per_iteration * args.iterations}")
    print(f"Output: {report_path}")
    print()

    # Run iterations
    all_results = []
    iteration_durations = []

    for i in range(1, args.iterations + 1):
        print(f"Iteration {i}/{args.iterations}...", end=" ", flush=True)
        results, duration = run_iteration(
            iteration=i,
            num_tags=args.tags,
            num_threads=args.threads,
            tables_per_iteration=args.tables_per_iteration,
        )
        all_results.extend(results)
        iteration_durations.append(duration)

        errors = len([r for r in results if not r["success"]])
        rps = len(results) / duration if duration > 0 else 0
        print(f"done in {duration:.2f}s ({len(results)} ALTERs, {errors} errors, {rps:.1f} ALTERs/sec)")

    print()

    # Write raw data
    with open(data_path, "w") as f:
        for r in all_results:
            f.write(json.dumps(r) + "\n")

    # Generate report
    generate_report(args, all_results, iteration_durations, profile_handler, report_path)

    print(f"Report written to: {report_path}")
    print(f"Raw data written to: {data_path}")
    print(f"Retry log written to: {log_path}")

    # Print summary to stdout
    success_lats = [r["latency_ms"] for r in all_results if r["success"]]
    if success_lats:
        s = latency_stats(success_lats)
        total_dur = sum(iteration_durations)
        print()
        print("=== Summary ===")
        print(f"  ALTERs: {len(all_results)} ({len(success_lats)} ok, {len(all_results) - len(success_lats)} failed)")
        print(f"  Latency: p50={s['p50']:.1f}ms  p90={s['p90']:.1f}ms  p95={s['p95']:.1f}ms  p99={s['p99']:.1f}ms  max={s['max']:.1f}ms")
        print(f"  Throughput: {len(all_results) / total_dur:.1f} ALTERs/sec")


if __name__ == "__main__":
    main()