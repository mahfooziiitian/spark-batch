# :material-console-line: Getting Started

This page walks through the first-run experience of the `spark-sql` CLI: verifying the install, launching the shell, loading the initial catalog state, running a query, and exiting cleanly.

Unlike Beeline, `spark-sql` creates its own Spark application. That means startup includes driver initialization and catalog discovery before the `Spark SQL>` prompt becomes ready for queries.

### :material-animation-play: Interactive Visualization — First Session Walkthrough

Step through the typical first-run flow to see what `spark-sql` is doing before the prompt appears, what becomes available at the prompt, and when session-scoped objects disappear.

<div id="viz-cli-getting-started" class="ts-viz"></div>

<script src="../../assets/js/configuration-cli-viz.js"></script>

______________________________________________________________________

## :material-download: Prerequisites

| Requirement          | Notes                                         |
| -------------------- | --------------------------------------------- |
| Apache Spark 4.x     | `spark-sql` ships in `$SPARK_HOME/bin/`       |
| Java 11 (LTS)        | Must be on `PATH`; set `JAVA_HOME`            |
| `SPARK_HOME` env var | Set to your Spark installation directory      |
| Hadoop (optional)    | Required for HDFS; not needed for local files |

```bash
# Verify the launcher is available
spark-sql --version
# output: Welcome to Spark version 4.x.y
```

______________________________________________________________________

## :material-play: Launching the Shell

```bash
# Default interactive mode
spark-sql

# Explicit local mode with 4 cores
spark-sql --master local[4]

# YARN client mode
spark-sql --master yarn --deploy-mode client

# Start directly in a database after running init SQL
spark-sql --database default -i ./sql/session_init.sql
```

When the session is ready, the prompt appears:

```
Spark SQL>
```

!!! note "Statement termination"

    In interactive mode, a statement is submitted only when `;` ends the line. Pressing Enter without a trailing semicolon continues the current statement instead of executing it.

______________________________________________________________________

## :material-keyboard: Interactive Mode

### First Queries

```sql
-- Inspect the current catalog
SHOW DATABASES;
USE default;
SHOW TABLES IN default;

-- Create and query a temporary view
CREATE OR REPLACE TEMP VIEW nums AS
SELECT *
FROM VALUES
    (1, 'a'),
    (2, 'b'),
    (3, 'c')
AS t (id, label);

SELECT * FROM nums;
```

### Show Column Headers

By default, `spark-sql` suppresses column headers. Enable them with the CLI-specific property:

```bash
spark-sql --conf "spark.sql.cli.print.header=true"
```

Or set it inside the current session:

```sql
SET spark.sql.cli.print.header=true;
```

### Built-In Shell Commands

The CLI also supports a few shell-style commands in interactive mode:

| Command           | Description                                     |
| ----------------- | ----------------------------------------------- |
| `quit;` / `exit;` | Exit the shell                                  |
| `source file.sql` | Run a script file from inside the current shell |
| `!pwd`            | Run a local shell command                       |
| `dfs -ls /path`   | Run an HDFS `dfs` command                       |

### Shell History and Editing

`spark-sql` uses `readline`, so familiar shell shortcuts work:

| Key                 | Action                                         |
| ------------------- | ---------------------------------------------- |
| `Up` / `Down`       | Navigate command history                       |
| `Ctrl+R`            | Reverse-search history                         |
| `Ctrl+A` / `Ctrl+E` | Jump to start / end of line                    |
| `Tab`               | Keyword or partial name completion             |
| `Ctrl+C`            | Cancel the current statement                   |
| `Ctrl+D`            | End input when appropriate in a shell pipeline |

______________________________________________________________________

## :material-file-find: Querying Files Directly

`spark-sql` can query files without first registering permanent tables:

```sql
-- Parquet directory
SELECT * FROM parquet.`./data/orders/` LIMIT 10;

-- CSV with inferred schema
SELECT * FROM csv.`./data/events.csv` LIMIT 5;

-- Newline-delimited JSON
SELECT * FROM json.`./data/logs/` LIMIT 5;
```

This is often the fastest way to validate a landing zone before deciding whether to create managed tables or views.

______________________________________________________________________

## :material-table-plus: Creating Tables from Files

```sql
-- External table backed by Parquet
CREATE TABLE IF NOT EXISTS sales_raw
USING PARQUET
LOCATION 'hdfs:///data/sales/raw';

DESCRIBE TABLE sales_raw;
SELECT COUNT(*) FROM sales_raw;

-- CTAS summary table
CREATE TABLE sales_summary
USING PARQUET
AS
SELECT
    region,
    SUM(amount) AS total_amount
FROM sales_raw
GROUP BY region;
```

______________________________________________________________________

## :material-magnify: Behavior Notes

1. **Session scope** — temp views and session `SET` values are lost when you exit the shell.
2. **Warehouse dir** — managed tables are written to `spark.sql.warehouse.dir`.
3. **Initialization files** — when you do not pass `-i`, Spark SQL can still load `.hiverc` files from the standard Hive locations.
4. **Comments and semicolons** — `;` only terminates a statement when it appears at the end of a line and is not escaped.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                        | Recommendation                                            |
| ------------------------------- | --------------------------------------------------------- |
| Explore a new dataset           | `spark-sql` with direct file queries                      |
| Quick ad-hoc aggregation        | Interactive mode                                          |
| Repeatable batch query          | `spark-sql -f script.sql` (see [Scripting](scripting.md)) |
| Remote Hive-compatible endpoint | `beeline` (see [Beeline](beeline.md))                     |
