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
        metrics["throughput"] = float(m.group(2))

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

            if report_type == "alter" and category == "column":
                cols = metrics.get("columns", "?")
                tags = metrics.get("tags", "?")
                label = f"ALTER column tags (c={cols}, t={tags})"
            elif report_type == "alter" and category == "table":
                tags = metrics.get("tags", "?")
                label = f"ALTER table tags (t={tags})"
            elif report_type == "info_schema" and category == "column":
                label = "info_schema column_tags SELECT"
            elif report_type == "info_schema" and category == "table":
                label = "info_schema table_tags SELECT"
            else:
                continue

            # Keep the one with more iterations
            existing = categories[category][label].get(threads)
            if existing and metrics.get("iterations", 0) <= existing.get("iterations", 0):
                continue

            categories[category][label][threads] = metrics
            print(f"  [{category}] {label} threads={threads}: "
                  f"wall={metrics.get('wall_clock_s', '?')}s, "
                  f"p50={metrics.get('p50', '?')}ms, "
                  f"throughput={metrics.get('throughput', '?')} ops/s "
                  f"[{fname}]")

    return categories


def plot_category(category_name, series, output_path):
    """Generate a 2x2 chart PNG for one category (column or table)."""
    if not series:
        print(f"  No data for {category_name}, skipping.")
        return

    # Color/style assignment
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

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    chart_configs = [
        (axes[0][0], "wall_clock_s", "Wall-Clock Time (seconds)", "Wall-Clock Time vs Thread Count"),
        (axes[0][1], "throughput", "Operations / second", "Throughput vs Thread Count"),
        (axes[1][0], "p50", "P50 Latency (ms)", "P50 Latency vs Thread Count"),
        (axes[1][1], "p99", "P99 Latency (ms)", "P99 Latency vs Thread Count"),
    ]

    for ax, metric_key, ylabel, title in chart_configs:
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

    title_label = "Column Tags" if category_name == "column" else "Table Tags"
    plt.suptitle(f"SET TAGS Profiling: {title_label} — info_schema SELECT vs Direct ALTER",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Chart saved to: {output_path}")


if __name__ == "__main__":
    print("Discovering results...\n")
    categories = discover_reports()

    total_series = sum(len(v) for v in categories.values())
    total_points = sum(len(td) for cat in categories.values() for td in cat.values())
    print(f"\nFound {total_series} series across {total_points} data points.\n")

    if "column" in categories:
        print("Generating column tags chart...")
        plot_category("column", categories["column"],
                      os.path.join(RESULTS_DIR, "comparison_column_tags.png"))

    if "table" in categories:
        print("Generating table tags chart...")
        plot_category("table", categories["table"],
                      os.path.join(RESULTS_DIR, "comparison_table_tags.png"))

    if not categories:
        print("No results found. Run experiments first.")
    else:
        print("\nDrag the PNGs into Google Docs.")
