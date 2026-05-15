"""Benchmark the kernel-backed connector against the Thrift backend.

One-shot script, not a CI gate. Runs each (backend × SQL-shape)
combination N+1 times against a live warehouse, drops the first
run (cache warm-up), and reports min / median / max wall-clock for
session-open, time-to-first-row, drain, and RSS delta.

Usage:

    set -a && source ~/.databricks/pecotesting-creds && set +a
    # If DATABRICKS_HOST is set but DATABRICKS_SERVER_HOSTNAME is
    # not, normalise it (matches the e2e suite's convention).
    export DATABRICKS_SERVER_HOSTNAME=${DATABRICKS_SERVER_HOSTNAME:-${DATABRICKS_HOST#https://}}
    .venv/bin/python scripts/bench_kernel_vs_thrift.py

Honest disclaimers:
- Single warehouse, single machine, single network route. High
  server-side variance is expected.
- Server-side caches warm differently between back-to-back runs;
  the first-run-drop helps but doesn't eliminate it.
- Comparison is **kernel-backed vs Thrift**. The pure-Python
  native SEA backend (``backend/sea/``) is no longer reachable via
  ``use_sea=True`` after this PR, so it's not included.
- RSS delta is process-wide and includes pyarrow tables we hold
  in scope during the drain. Two-orders-of-magnitude differences
  are signal; 10% differences are noise.

The output is a Markdown table you can paste into a PR
description.
"""

from __future__ import annotations

import argparse
import gc
import os
import resource
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import databricks.sql as sql


# ─── Config ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class Shape:
    name: str
    sql: Optional[str]                # None means it's a metadata call
    metadata_call: Optional[str]      # e.g. "catalogs"
    expected_rows: Optional[int]      # None when we don't assert


SHAPES: List[Shape] = [
    Shape("SELECT 1", "SELECT 1 AS n", None, 1),
    Shape("range(10k)", "SELECT * FROM range(10000)", None, 10_000),
    Shape("range(1M)", "SELECT * FROM range(1000000)", None, 1_000_000),
    Shape(
        "wide-uuid(100k)",
        "SELECT id, uuid() AS u FROM range(100000)",
        None,
        100_000,
    ),
    Shape("metadata.catalogs", None, "catalogs", None),
]


BACKENDS: List[Tuple[str, Dict]] = [
    ("thrift", {"use_sea": False}),
    ("kernel", {"use_sea": True}),
]


# ─── Measurement ─────────────────────────────────────────────────


@dataclass
class SampleMetrics:
    open_s: float
    ttfr_s: float
    drain_s: float
    rows: int
    rss_delta_kb: int


def _rss_kb() -> int:
    # ru_maxrss is in KB on Linux, bytes on macOS — the script is
    # primarily for Linux CI / dev shells, document the macOS
    # caveat and move on.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def run_one(backend_kwargs: Dict, shape: Shape, conn_params: Dict) -> SampleMetrics:
    """Open a fresh connection, run the shape, drain, return metrics."""
    gc.collect()
    rss_before = _rss_kb()

    t0 = time.perf_counter()
    conn = sql.connect(**conn_params, **backend_kwargs)
    t_open = time.perf_counter()
    try:
        cur = conn.cursor()
        try:
            t_pre_exec = time.perf_counter()
            if shape.sql is not None:
                cur.execute(shape.sql)
            else:
                getattr(cur, shape.metadata_call)()
            # First row marks the end of poll + first-fetch latency.
            first = cur.fetchmany(1)
            t_ttfr = time.perf_counter()
            # Drain the rest.
            tail_rows = 0
            while True:
                chunk = cur.fetchmany(10_000)
                if not chunk:
                    break
                tail_rows += len(chunk)
            t_drain = time.perf_counter()
            total_rows = len(first) + tail_rows
            if shape.expected_rows is not None and total_rows != shape.expected_rows:
                raise RuntimeError(
                    f"{shape.name}: expected {shape.expected_rows} rows, got {total_rows}"
                )
        finally:
            cur.close()
    finally:
        conn.close()

    rss_after = _rss_kb()
    return SampleMetrics(
        open_s=t_open - t0,
        ttfr_s=t_ttfr - t_pre_exec,
        drain_s=t_drain - t_pre_exec,
        rows=total_rows,
        rss_delta_kb=max(0, rss_after - rss_before),
    )


# ─── Aggregation ─────────────────────────────────────────────────


@dataclass
class Aggregated:
    open_min: float
    open_med: float
    open_max: float
    ttfr_min: float
    ttfr_med: float
    ttfr_max: float
    drain_min: float
    drain_med: float
    drain_max: float
    rows: int
    rss_med_kb: int


def aggregate(samples: List[SampleMetrics]) -> Aggregated:
    o = [s.open_s for s in samples]
    t = [s.ttfr_s for s in samples]
    d = [s.drain_s for s in samples]
    r = [s.rss_delta_kb for s in samples]
    return Aggregated(
        open_min=min(o), open_med=statistics.median(o), open_max=max(o),
        ttfr_min=min(t), ttfr_med=statistics.median(t), ttfr_max=max(t),
        drain_min=min(d), drain_med=statistics.median(d), drain_max=max(d),
        rows=samples[0].rows,
        rss_med_kb=int(statistics.median(r)),
    )


def fmt_ms(seconds: float) -> str:
    return f"{seconds * 1000:.0f}"


def fmt_rate(rows: int, seconds: float) -> str:
    if seconds <= 0:
        return "—"
    return f"{int(rows / seconds):,}"


# ─── Driver ──────────────────────────────────────────────────────


def build_conn_params() -> Dict:
    host = os.environ.get("DATABRICKS_SERVER_HOSTNAME") or os.environ.get("DATABRICKS_HOST", "")
    host = host.replace("https://", "").rstrip("/")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not (host and http_path and token):
        sys.exit(
            "Missing credentials. Set DATABRICKS_SERVER_HOSTNAME (or _HOST), "
            "DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN before running."
        )
    return {
        "server_hostname": host,
        "http_path": http_path,
        "access_token": token,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--samples", type=int, default=5,
        help="Sample runs per (backend, shape). First run is dropped as warm-up. Default: 5.",
    )
    parser.add_argument(
        "--shapes", nargs="*",
        help="Subset of shapes to run by name. Default: all. Choices: " +
             ", ".join(s.name for s in SHAPES),
    )
    parser.add_argument(
        "--backends", nargs="*", choices=[b for b, _ in BACKENDS],
        help="Subset of backends. Default: both.",
    )
    args = parser.parse_args()

    conn_params = build_conn_params()
    shapes = [s for s in SHAPES if not args.shapes or s.name in args.shapes]
    backends = [(n, k) for (n, k) in BACKENDS if not args.backends or n in args.backends]

    if not shapes:
        sys.exit(f"No shapes match {args.shapes!r}")
    if not backends:
        sys.exit(f"No backends match {args.backends!r}")

    # results[(shape_name, backend_name)] = Aggregated
    results: Dict[Tuple[str, str], Aggregated] = {}

    total_runs = len(shapes) * len(backends) * (args.samples + 1)
    print(f"Running {total_runs} samples ({len(shapes)} shapes × {len(backends)} backends × {args.samples + 1} runs/cell)\n", flush=True)

    for shape in shapes:
        for backend_name, backend_kwargs in backends:
            print(f"  {shape.name:24s} on {backend_name:8s} … ", end="", flush=True)
            samples: List[SampleMetrics] = []
            # +1 because we drop the first run.
            for run_idx in range(args.samples + 1):
                try:
                    m = run_one(backend_kwargs, shape, conn_params)
                except Exception as exc:
                    print(f"\n    run {run_idx} FAILED: {exc}", flush=True)
                    raise
                if run_idx == 0:
                    continue  # warmup
                samples.append(m)
            agg = aggregate(samples)
            results[(shape.name, backend_name)] = agg
            print(
                f"open={fmt_ms(agg.open_med)}ms  "
                f"ttfr={fmt_ms(agg.ttfr_med)}ms  "
                f"drain={fmt_ms(agg.drain_med)}ms  "
                f"rows={agg.rows:,}  "
                f"rss+={agg.rss_med_kb}kb",
                flush=True,
            )

    # ─── Report ─────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("Results (median across {} samples; warm-up dropped):".format(args.samples))
    print("=" * 70)
    for shape in shapes:
        print(f"\n### {shape.name}")
        if shape.metadata_call:
            print(f"_metadata: cursor.{shape.metadata_call}()_")
        else:
            print(f"_SQL: `{shape.sql}`_")
        print()
        print("| backend | open (ms) | ttfr (ms) | drain (ms) | rows/s | rss Δ (KB) |")
        print("|---|---|---|---|---|---|")
        for backend_name, _ in backends:
            agg = results.get((shape.name, backend_name))
            if agg is None:
                print(f"| {backend_name} | (skipped) | | | | |")
                continue
            print(
                f"| {backend_name} | "
                f"{fmt_ms(agg.open_med)} ({fmt_ms(agg.open_min)}–{fmt_ms(agg.open_max)}) | "
                f"{fmt_ms(agg.ttfr_med)} ({fmt_ms(agg.ttfr_min)}–{fmt_ms(agg.ttfr_max)}) | "
                f"{fmt_ms(agg.drain_med)} ({fmt_ms(agg.drain_min)}–{fmt_ms(agg.drain_max)}) | "
                f"{fmt_rate(agg.rows, agg.drain_med)} | "
                f"{agg.rss_med_kb} |"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
