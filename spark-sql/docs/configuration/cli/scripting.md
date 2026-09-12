# :material-script-text: Scripting

Use `spark-sql` in non-interactive mode when SQL should behave like a batch artifact: checked into source control, parameterised at launch, and executed without waiting at a prompt.

Three patterns matter most in practice: reading a `.sql` file with `-f`, streaming SQL through stdin, and mixing a reusable initializer file with either interactive or batch execution. They all use the same parser, but they differ in how input arrives and when the process returns an exit code.

### :material-animation-play: Interactive Visualization — Batch vs Prompted Execution

Compare script-file, stdin, and interactive execution to see which input source Spark consumes, whether the prompt appears, and when stdout and the shell exit status become available to automation.

<div id="viz-cli-scripting" class="ts-viz"></div>

<script src="../../assets/js/configuration-cli-viz.js"></script>

______________________________________________________________________

## :material-play-circle: Running a Script File

```bash
spark-sql -f ./sql/script.sql
```

Spark reads semicolon-terminated statements in order, prints query results to stdout, and exits when the file is exhausted or a statement fails.

For machine-consumed output, add silent mode so the result stream contains less banner noise:

```bash
spark-sql -S -f ./sql/script.sql
```

______________________________________________________________________

## :material-file-document-edit: Script File Conventions

```sql
-- script.sql

-- 1. Set session options at the top
SET spark.sql.adaptive.enabled = true;
SET spark.sql.cli.print.header = true;

-- 2. Create or refresh a staging view
CREATE OR REPLACE TEMP VIEW raw_orders AS
SELECT *
FROM parquet.`./data/orders/`
WHERE order_date >= '2026-01-01';

-- 3. Transform
CREATE OR REPLACE TEMP VIEW clean_orders AS
SELECT
    order_id,
    LOWER(TRIM(region)) AS region,
    CAST(amount AS DOUBLE) AS amount,
    CAST(order_date AS DATE) AS order_date
FROM raw_orders
WHERE order_id IS NOT NULL;

-- 4. Write result
INSERT OVERWRITE TABLE analytics.daily_orders
SELECT
    region,
    order_date,
    SUM(amount) AS total_amount
FROM clean_orders
GROUP BY region, order_date;
```

______________________________________________________________________

## :material-variable: Passing Variables with `--hivevar`, `--define`, and `--hiveconf`

Use `--hivevar` for script parameters, `--define` for Hive-style shorthand substitution, and `--hiveconf` when the same launch also needs Hive properties.

```bash
spark-sql   --hivevar START_DATE=2026-01-01   --hivevar END_DATE=2026-01-31   --define TARGET_DB=analytics   --hiveconf hive.exec.dynamic.partition.mode=nonstrict   -f ./sql/load_orders.sql
```

Inside the script:

```sql
USE ${TARGET_DB};

INSERT OVERWRITE TABLE monthly_summary
SELECT
    region,
    SUM(amount) AS total_amount
FROM orders
WHERE order_date BETWEEN '${hivevar:START_DATE}' AND '${hivevar:END_DATE}'
GROUP BY region;
```

!!! tip "Quote date variables in SQL"

    Wrap `${hivevar:date_var}` in single quotes when substituting into string or date literals.

______________________________________________________________________

## :material-console-line: Initialisation File with `-i`

An initialization file lets you preload session settings, helper views, and shared SQL before the main work begins:

```sql
-- init.sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.cli.print.header = true;

CREATE OR REPLACE TEMP VIEW fiscal_calendar AS
SELECT * FROM parquet.`./ref/fiscal_calendar/`;
```

```bash
spark-sql -i ./sql/init.sql -f ./sql/main_script.sql
```

You can also combine `-i` with plain interactive mode when analysts want a prompt that already has common views and settings loaded.

______________________________________________________________________

## :material-pipe: Piping SQL via stdin

```bash
# Here-doc
spark-sql <<'EOF'
SELECT region, COUNT(*) AS cnt
FROM sales
GROUP BY region
ORDER BY cnt DESC;
EOF

# Pipe from another command
printf '%s\\n' "SELECT current_date();" | spark-sql -S
```

stdin is useful when another tool generates SQL on the fly. Once the input stream ends, `spark-sql` exits and returns a normal shell status code.

______________________________________________________________________

## :material-source-branch: Running Scripts from Inside the Shell

If you are already in interactive mode, use `source` to execute another file without starting a new process:

```sql
source ./sql/daily_checks.sql;
```

This keeps the same session state, including temp views, current database, and `SET` values created earlier in the shell.

______________________________________________________________________

## :material-check-all: Multi-Statement Scripts and Error Handling

By default, `spark-sql` stops on the first error. Keep scripts idempotent with guards such as `IF EXISTS`, `CREATE OR REPLACE`, and repeatable staging patterns:

```sql
DROP TABLE IF EXISTS staging.temp_load;

CREATE TABLE staging.temp_load
USING PARQUET
AS
SELECT *
FROM raw_orders
WHERE order_date = '${hivevar:LOAD_DATE}';

INSERT OVERWRITE TABLE analytics.orders_snapshot
SELECT *
FROM staging.temp_load;

DROP TABLE IF EXISTS staging.temp_load;
```

______________________________________________________________________

## :material-clock-fast: Scheduling with cron or Airflow

```bash
#!/usr/bin/env bash
set -euo pipefail

LOAD_DATE=$(date -d "yesterday" +%Y-%m-%d)

spark-sql   --master yarn   --executor-memory 8g   --num-executors 20   --conf spark.sql.adaptive.enabled=true   --hivevar LOAD_DATE="${LOAD_DATE}"   --define TARGET_DB=analytics   -f ./sql/daily_load.sql   2>&1 | tee "./logs/daily_load_${LOAD_DATE}.log"
```

Run from cron:

```cron
0 2 * * * /opt/scripts/run_daily_load.sh
```

______________________________________________________________________

## :material-brain: When to Use

| Scenario             | Recommendation                       |
| -------------------- | ------------------------------------ |
| Daily ETL job        | `spark-sql -f` in cron or Airflow    |
| Parameterised load   | `--hivevar` or `--define`            |
| Shared session setup | `-i init.sql` before the main script |
| Generated SQL input  | Pipe through stdin                   |
| One-off exploration  | Interactive mode (`spark-sql`)       |
