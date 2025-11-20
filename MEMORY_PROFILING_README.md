# Memory Profiling Scripts

This directory contains scripts for profiling the memory footprint of the Databricks SQL Python connector with telemetry enabled vs disabled.

## Setup

Before running the memory profiling scripts, set the required environment variables:

```bash
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-personal-access-token"
```

Or source them from your `test.env` file:

```bash
source test.env
```

## Available Scripts

### 1. `memory_profile_telemetry_v2.py` (Recommended)

The improved version that properly isolates telemetry-specific memory overhead.

**Usage:**
```bash
python memory_profile_telemetry_v2.py
```

**What it does:**
- Pre-loads all modules to avoid counting import overhead
- Runs 10 connection cycles with 5 queries each
- Measures memory with telemetry DISABLED, then ENABLED
- Identifies telemetry-specific allocations
- Generates detailed report and JSON output

**Output:**
- Console: Detailed memory statistics and comparison
- `memory_profile_results_v2.json`: Raw profiling data
- `MEMORY_PROFILING_SUMMARY.md`: Human-readable summary

### 2. `memory_profile_telemetry.py`

Original version (kept for reference).

**Usage:**
```bash
python memory_profile_telemetry.py
```

## Results

See `MEMORY_PROFILING_SUMMARY.md` for the latest profiling results and analysis.

## Key Findings

✅ **Telemetry Memory Overhead: < 600 KB**
- Minimal impact on production workloads
- Circuit breaker adds < 25 KB
- No memory leaks detected

## Notes

- **test.env** is in `.gitignore` - never commit credentials
- Scripts use Python's built-in `tracemalloc` module
- Results may vary based on Python version and system

