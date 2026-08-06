# SET TAGS Profiling Scripts

## Context

These scripts measure the performance of two approaches for managing tags on Databricks tables and columns:

**Approach A (direct ALTER)**: Call `ALTER TABLE SET TAGS` or `ALTER TABLE ALTER COLUMN SET TAGS` directly, overwriting any existing tags without reading them first. SET TAGS is idempotent — setting the same key overwrites the value.

**Approach B (read then write)**: First query `system.information_schema.column_tags` or `system.information_schema.table_tags` to read existing tags, compute a diff, then issue ALTERs only for changes.

The goal is to determine whether the information_schema read step is worth the cost, or whether direct ALTER is faster even though it may redundantly set unchanged tags.

## Prerequisites

- Python 3.x with `databricks-sql-connector` installed (`pip install -e .` from repo root)
- A Databricks SQL warehouse
- 64 tables (`table1` through `table64`) with 128 STRING columns each. Create them by running `profile_column_tags.py` without `--skip-setup`.
- Credentials in `examples/credentials.env` (gitignored). Copy and edit:

```bash
cp examples/credentials.env.example examples/credentials.env
# Edit credentials.env with your workspace details
```

The file format is:
```
SERVER_HOSTNAME=your-workspace.cloud.databricks.com
HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
ACCESS_TOKEN=your_token
CATALOG=your_catalog
SCHEMA=your_schema
```

All scripts read from this file via `load_credentials.py`. To switch workspaces, just edit `credentials.env`.

## Scripts

### profile_column_tags.py — Direct ALTER column tags

Sets tags on columns directly via ALTER statements. No information_schema reads.

```bash
# Create tables + validate
python examples/profile_column_tags.py --columns 1 --tags 1 --threads 1 --iterations 1 --validate

# Full experiment: 100 columns, 9 tags each, 8 threads, 3 iterations
python examples/profile_column_tags.py --columns 100 --tags 9 --threads 8 --iterations 3 --skip-setup
```

Arguments:
- `--columns`: Number of columns to tag per table
- `--tags`: Number of tags per ALTER command
- `--threads`: Concurrent connections
- `--iterations`: Times to repeat the full 64-table sweep
- `--skip-setup`: Skip table creation
- `--validate`: Force 1 iteration for quick validation

Output: `examples/results/column_tags/`

### profile_table_tags.py — Direct ALTER table tags

Sets tags on tables directly via ALTER statements. One ALTER per table.

```bash
python examples/profile_table_tags.py --tags 1 --threads 8 --iterations 3
```

Arguments:
- `--tags`: Number of tags per ALTER command
- `--threads`: Concurrent connections
- `--iterations`: Times to repeat the full 64-table sweep
- `--validate`: Force 1 iteration

Output: `examples/results/table_tags/`

### profile_read_then_write_tags.py — information_schema column_tags SELECT

Queries `system.information_schema.column_tags` for each table. No ALTER — measures the read cost only.

```bash
python examples/profile_read_then_write_tags.py --threads 1 --iterations 3
```

Arguments:
- `--threads`: Concurrent connections
- `--iterations`: Times to repeat the full 64-table sweep
- `--validate`: Force 1 iteration

Output: `examples/results/read_then_write/`

### profile_read_then_write_table_tags.py — information_schema table_tags SELECT

Queries `system.information_schema.table_tags` for each table. No ALTER — measures the read cost only.

```bash
python examples/profile_read_then_write_table_tags.py --threads 1 --iterations 3
```

Arguments:
- `--threads`: Concurrent connections
- `--iterations`: Times to repeat the full 64-table sweep
- `--validate`: Force 1 iteration

Output: `examples/results/read_then_write_table_tags/`

### cleanup_column_tags.py — Remove all tags

Removes all column tags and table tags from all 64 tables using 32 threads. Run this to reset state between experiments.

```bash
python examples/cleanup_column_tags.py
```

### plot_comparison.py — Generate charts

Reads all report files and generates comparison charts as PNGs.

```bash
pip install matplotlib
python examples/plot_comparison.py
```

Output:
- `examples/results/comparison_column_tags.png` — column tags: ALTER vs info_schema
- `examples/results/comparison_table_tags.png` — table tags: ALTER vs info_schema

Each PNG has 4 charts: wall-clock time, throughput, P50 latency, P99 latency, all plotted against thread count.

## Running the definitive experiment

```bash
# Step 1: Create tables (once)
python examples/profile_column_tags.py --columns 1 --tags 1 --threads 1 --iterations 1 --validate

# Step 2: Run info_schema reads across thread counts
# Stop early if latency is already unacceptable
for n in 1 2 4 8 16 32 64; do
  python examples/profile_read_then_write_tags.py --threads $n --iterations 3
done

# Step 3: Run direct ALTERs across thread counts
for n in 1 2 4 8 16 32 64; do
  python examples/profile_column_tags.py --columns 100 --tags 9 --threads $n --iterations 3 --skip-setup
done

# Step 4: Generate charts
python examples/plot_comparison.py
```

## Connector instrumentation

The scripts capture retry behavior via `[PROFILE]` log lines added to the connector:
- `src/databricks/sql/backend/thrift_backend.py` — logs per-attempt timing, success, statement IDs, and retry sleeps in `make_request()`
- `src/databricks/sql/auth/retry.py` — logs urllib3-level retry decisions (`should_retry`) and sleep durations with HTTP status codes, Thrift method names, and SQL text

These are written to `*_retries.log` files alongside each report. Use `grep "[PROFILE]"` to filter.

## Output structure

Each script run produces three files:
- `*_report.md` — Markdown report with latency percentiles, throughput, error analysis, retry analysis
- `*_data.jsonl` — Raw per-operation data (one JSON line per ALTER or SELECT)
- `*_retries.log` — Full connector debug logs with `[PROFILE]` instrumentation

## Key finding

On Azure workspaces, `system.information_schema.column_tags` queries can take 60-110 seconds under concurrency due to server-side queuing (visible as repeated `GetOperationStatus` polling in logs). Direct ALTER SET TAGS consistently completes in ~500ms regardless of concurrency. The information_schema read alone is slower than performing all the writes it was meant to optimize.