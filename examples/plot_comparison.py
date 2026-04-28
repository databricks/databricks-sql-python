#!/usr/bin/env python3
"""
Plot all profiling results: info_schema SELECTs vs direct ALTERs.

Auto-discovers all report MD files across all result directories.
Generates SEPARATE PNGs for column tags and table tags.
Each has 4 charts: wall-clock, throughput, P50, P99.

Usage:
    python examples/plot_comparison.py
"""

import re
import os
import sys
from collections import defaultdict

sys.stdout.reconfigure(line_buffering=True)

try:
    import matplotlib.pyplot as plt
except ImportError:
    print("Install matplotlib: pip install matplotlib")
    sys.exit(1)

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def parse_report(filepath):
    """Extract key metrics from a report MD file."""
    metrics = {}
    with open(filepath) as f:
        content = f.read()

    m = re.search(r"\*\*Total wall-clock\*\*:\s*([\d.]+)s", content)
    if m:
        metrics["wall_clock_s"] = float(m.group(1))

    m = re.search(r"\*\*(ALTERs/sec|SELECTs/sec|Operations/sec)\*\*:\s*([\d.]+)", content)
    if m:
        metrics["throughput_ops"] = float(m.group(2))

    for pct in ["p50", "p90", "p95", "p99"]:
        m = re.search(rf"\|\s*{pct}\s*\|\s*([\d.]+)\s*\|", content)
        if m:
            metrics[pct] = float(m.group(1))

    m = re.search(r"\|\s*max\s*\|\s*([\d.]+)\s*\|", content)
    if m:
        metrics["max"] = float(m.group(1))

    m = re.search(r"\|\s*count\s*\|\s*([\d.]+)\s*\|", content)
    if m:
        metrics["count"] = int(float(m.group(1)))

    m = re.search(r"\*\*Threads\*\*:\s*(\d+)", content)
    if m:
        metrics["threads"] = int(m.group(1))

    m = re.search(r"\*\*Iterations\*\*:\s*(\d+)", content)
    if m:
        metrics["iterations"] = int(m.group(1))

    m = re.search(r"\*\*Columns tagged per table\*\*:\s*(\d+)", content)
    if m:
        metrics["columns"] = int(m.group(1))

    m = re.search(r"\*\*Tables per iteration\*\*:\s*(\d+)", content)
    if m:
        metrics["tables_per_iteration"] = int(m.group(1))

    # Also match older reports that used "Tables": N
    if "tables_per_iteration" not in metrics:
        m = re.search(r"\*\*Total SELECTs\*\*:\s*(\d+)", content)
        iters = metrics.get("iterations", 1)
        if m and iters:
            metrics["tables_per_iteration"] = int(float(m.group(1))) // iters

    m = re.search(r"\*\*Tags per ALTER\*\*:\s*(\d+)", content)
    if m:
        metrics["tags"] = int(m.group(1))

    return metrics


def classify_report(dirpath, filename):
    """Classify a report: (category, type) where category is 'column' or 'table'."""
    dirpath_lower = dirpath.lower()
    filename_lower = filename.lower()

    if "read_then_write_table_tags" in dirpath_lower or filename_lower.startswith("rwtt_"):
        return "table", "info_schema"
    elif "read_then_write" in dirpath_lower or filename_lower.startswith("rw_"):
        return "column", "info_schema"
    elif "table_tags" in dirpath_lower or filename_lower.startswith("tt_"):
        return "table", "alter"
    elif "column_tags" in dirpath_lower or filename_lower.startswith("c"):
        return "column", "alter"
    else:
        return "unknown", "unknown"


def discover_reports():
    """Walk all result directories and collect report data, split by category."""
    # {category: {series_label: {threads: metrics}}}
    categories = defaultdict(lambda: defaultdict(dict))

    for dirpath, _, filenames in os.walk(RESULTS_DIR):
        for fname in sorted(filenames):
            if not fname.endswith("_report.md"):
                continue

            filepath = os.path.join(dirpath, fname)
            metrics = parse_report(filepath)

            if "threads" not in metrics:
                continue

            category, report_type = classify_report(dirpath, fname)
            if category == "unknown":
                continue

            threads = metrics["threads"]

            tbl = metrics.get("tables_per_iteration", "?")

            if report_type == "alter" and category == "column":
                cols = metrics.get("columns", "?")
                tags = metrics.get("tags", "?")
                label = f"ALTER column tags (columns={cols}, tags_per_column={tags}, tables={tbl})"
            elif report_type == "alter" and category == "table":
                tags = metrics.get("tags", "?")
                label = f"ALTER table tags (tags={tags}, tables={tbl})"
            elif report_type == "info_schema" and category == "column":
                label = f"info_schema column_tags SELECT (tables={tbl})"
            elif report_type == "info_schema" and category == "table":
                label = f"info_schema table_tags SELECT (tables={tbl})"
            else:
                continue

            # Keep the one with more iterations
            existing = categories[category][label].get(threads)
            if existing and metrics.get("iterations", 0) <= existing.get("iterations", 0):
                continue

            # Compute tables/sec from wall-clock and tables_per_iteration
            tpi = metrics.get("tables_per_iteration")
            wc = metrics.get("wall_clock_s")
            if tpi and wc and wc > 0:
                metrics["tables_per_sec"] = round(tpi / wc, 2)

            categories[category][label][threads] = metrics
            print(f"  [{category}] {label} threads={threads}: "
                  f"wall={metrics.get('wall_clock_s', '?')}s, "
                  f"p50={metrics.get('p50', '?')}ms, "
                  f"tables/s={metrics.get('tables_per_sec', '?')} "
                  f"[{fname}]")

    return categories


def build_style_map(series):
    """Assign colors and styles to series labels."""
    colors_info = ["#d62728", "#ff7f0e"]
    colors_alter = ["#1f77b4", "#2ca02c", "#9467bd", "#17becf", "#8c564b"]
    info_idx = 0
    alter_idx = 0
    style_map = {}

    for label in sorted(series.keys()):
        if "info_schema" in label:
            style_map[label] = {"color": colors_info[info_idx % len(colors_info)], "marker": "o", "linestyle": "--"}
            info_idx += 1
        else:
            style_map[label] = {"color": colors_alter[alter_idx % len(colors_alter)], "marker": "s", "linestyle": "-"}
            alter_idx += 1

    return style_map


def plot_charts(series, style_map, chart_configs, suptitle, output_path):
    """Generate a chart PNG with len(chart_configs) subplots."""
    n = len(chart_configs)
    cols = 2
    rows = (n + 1) // 2
    fig, axes = plt.subplots(rows, cols, figsize=(16, 6 * rows))
    if rows == 1:
        axes = [axes]

    for idx, (metric_key, ylabel, title) in enumerate(chart_configs):
        ax = axes[idx // cols][idx % cols]
        for label, thread_data in sorted(series.items()):
            threads = sorted(thread_data.keys())
            values = [thread_data[t].get(metric_key) for t in threads]
            if any(v is not None for v in values):
                s = style_map[label]
                ax.plot(threads, values, marker=s["marker"], linestyle=s["linestyle"],
                        color=s["color"], linewidth=2, label=label, markersize=8)
        ax.set_xlabel("Thread Count")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    # Hide unused subplot if odd number of charts
    if n % 2 == 1:
        axes[rows - 1][1].set_visible(False)

    plt.suptitle(suptitle, fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Chart saved to: {output_path}")


def plot_category(category_name, series, output_dir):
    """Generate two PNGs per category: table-level comparison + individual operation detail."""
    if not series:
        print(f"  No data for {category_name}, skipping.")
        return

    style_map = build_style_map(series)
    title_label = "Column Tags" if category_name == "column" else "Table Tags"

    # Chart 1: Table-level comparison (apples-to-apples across approaches)
    table_charts = [
        ("wall_clock_s", "Wall-Clock Time (seconds)", "Wall-Clock Time vs Thread Count (Lower is better)"),
        ("tables_per_sec", "Tables / second", "Tables Processed per Second vs Thread Count (Higher is better)"),
    ]
    plot_charts(
        series, style_map, table_charts,
        f"{title_label}: Table-Level Comparison — info_schema SELECT vs Direct ALTER",
        os.path.join(output_dir, f"comparison_{category_name}_tags_tables.png"),
    )

    # Chart 2: Individual operation detail (per-op latency)
    op_charts = [
        ("throughput_ops", "Individual Operations / second", "Individual Op Throughput vs Thread Count (Higher is better)"),
        ("p50", "P50 Latency per Op (ms)", "P50 Latency vs Thread Count (Lower is better)"),
        ("p99", "P99 Latency per Op (ms)", "P99 Latency vs Thread Count (Lower is better)"),
        ("max", "Max Latency per Op (ms)", "Max Latency vs Thread Count (Lower is better)"),
    ]
    plot_charts(
        series, style_map, op_charts,
        f"{title_label}: Individual Operation Detail",
        os.path.join(output_dir, f"comparison_{category_name}_tags_ops.png"),
    )


if __name__ == "__main__":
    print("Discovering results...\n")
    categories = discover_reports()

    total_series = sum(len(v) for v in categories.values())
    total_points = sum(len(td) for cat in categories.values() for td in cat.values())
    print(f"\nFound {total_series} series across {total_points} data points.\n")

    if "column" in categories:
        print("Generating column tags charts...")
        plot_category("column", categories["column"], RESULTS_DIR)

    if "table" in categories:
        print("Generating table tags charts...")
        plot_category("table", categories["table"], RESULTS_DIR)

    if not categories:
        print("No results found. Run experiments first.")
    else:
        print("\nDrag the PNGs into Google Docs.")
