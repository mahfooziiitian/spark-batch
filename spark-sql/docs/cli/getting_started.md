# :material-console-line: Getting Started

This page walks through installing, launching, and running your first queries
with the `spark-sql` CLI.

---

## :material-download: Prerequisites

| Requirement | Notes |
|-------------|-------|
| Apache Spark 3.x | `spark-sql` ships in `$SPARK_HOME/bin/` |
| Java 11 (LTS) | Must be on `PATH`; set `JAVA_HOME` |
| `SPARK_HOME` env var | Set to your Spark installation directory |
| Hadoop (optional) | Required for HDFS; not needed for local files |

```bash
# Verify
spark-sql --version
# output: Welcome to Spark version 3.5.x
```

---

## :material-play: Launching the Shell

```bash
# Default — local mode, all CPU cores
spark-sql

# Explicit local mode with 4 cores
spark-sql --master local[4]

# YARN cluster
spark-sql --master yarn --deploy-mode client

# Kubernetes
spark-sql --master k8s://https://k8s-api:443 \
          --conf spark.kubernetes.container.image=my-spark:3.5
```

The interactive prompt appears:

```
Spark SQL>
```

---

## :material-keyboard: Interactive Mode

### First Queries

```sql
-- List available databases
SHOW DATABASES;

-- Select a database
USE default;

-- Create and query an in-memory table
CREATE OR REPLACE TEMP VIEW nums AS
SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c') AS t(id, label);

SELECT * FROM nums;
-- id | label
-- ---|------
-- 1  | a
-- 2  | b
-- 3  | c
```

### Show Column Headers

By default `spark-sql` suppresses column headers. Enable them:

```bash
spark-sql --conf "spark.hadoop.hive.cli.print.header=true"
```

Or set it inside the session:

```sql
SET spark.hadoop.hive.cli.print.header=true;
```

### Shell History and Editing

`spark-sql` uses `readline` — the same key bindings as `bash`:

| Key | Action |
|-----|--------|
| `Up` / `Down` | Navigate command history |
| `Ctrl+R` | Reverse-search history |
| `Ctrl+A` / `Ctrl+E` | Jump to start / end of line |
| `Tab` | Keyword/table name completion (partial support) |
| `Ctrl+C` | Cancel current statement |
| `quit;` or `exit;` | Exit the shell |

---

## :material-file-find: Querying Files Directly

`spark-sql` can query files without creating tables:

```sql
-- Parquet
SELECT * FROM parquet.`/mnt/data/orders/` LIMIT 10;

-- CSV (infer schema)
SELECT * FROM csv.`/mnt/data/events.csv` LIMIT 5;

-- JSON (newline-delimited)
SELECT * FROM json.`/mnt/data/logs/` LIMIT 5;

-- Delta
SELECT * FROM delta.`/mnt/delta/customers/` LIMIT 10;
```

---

## :material-table-plus: Creating Tables from Files

```sql
-- External table backed by Parquet on HDFS
CREATE TABLE IF NOT EXISTS sales_raw
USING PARQUET
LOCATION 'hdfs:///data/sales/raw';

DESCRIBE TABLE sales_raw;
SELECT COUNT(*) FROM sales_raw;

-- CTAS from a file query
CREATE TABLE sales_summary
USING DELTA
AS
SELECT region, SUM(amount) AS total
FROM parquet.`/mnt/data/sales/`
GROUP BY region;
```

---

## :material-magnify: Behavior Notes

1. **Session scope** — temp views and temp functions are lost when you exit the shell.
2. **Warehouse dir** — managed tables are written to `spark.sql.warehouse.dir` (default: `./spark-warehouse`).
3. **No ANSI mode by default** — Spark SQL allows some non-standard syntax; enable with `SET spark.sql.ansi.enabled=true`.
4. **Multi-line statements** — a statement is complete only when terminated with `;`. Press Enter mid-statement to continue on a new line.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Explore a new dataset | `spark-sql` — direct file queries |
| Quick ad-hoc aggregation | Interactive mode |
| Automated nightly script | `spark-sql -f script.sql` (see [Scripting](scripting.md)) |
| Remote Hive cluster | `beeline` (see [Beeline](beeline.md)) |
