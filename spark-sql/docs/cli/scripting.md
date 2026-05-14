# :material-script-text: Scripting

Run SQL non-interactively using `.sql` files — ideal for ETL pipelines,
scheduled jobs, and CI/CD workflows.

---

## :material-play-circle: Running a Script File

```bash
spark-sql -f /path/to/script.sql
```

The shell executes each `;`-terminated statement in order, prints results to
stdout, and exits when the file is exhausted (or on the first error).

---

## :material-file-document-edit: Script File Conventions

```sql
-- script.sql

-- 1. Set session options at the top
SET spark.sql.adaptive.enabled = true;
SET spark.hadoop.hive.cli.print.header = true;

-- 2. Create or refresh a staging view
CREATE OR REPLACE TEMP VIEW raw_orders AS
SELECT *
FROM parquet.`/mnt/data/orders/`
WHERE order_date >= '2024-01-01';

-- 3. Transform
CREATE OR REPLACE TEMP VIEW clean_orders AS
SELECT
    order_id,
    LOWER(TRIM(region))          AS region,
    CAST(amount AS DOUBLE)       AS amount,
    CAST(order_date AS DATE)     AS order_date
FROM raw_orders
WHERE order_id IS NOT NULL;

-- 4. Write result
INSERT OVERWRITE TABLE analytics.daily_orders
SELECT region, order_date, SUM(amount) AS total
FROM clean_orders
GROUP BY region, order_date;
```

---

## :material-variable: Passing Variables with `--hivevar` and `--hiveconf`

`spark-sql` inherits Hive CLI variable substitution. Use `--hivevar` for
script variables and reference them as `${hivevar:name}`.

```bash
spark-sql \
  --hivevar START_DATE=2024-01-01 \
  --hivevar END_DATE=2024-06-30 \
  --hivevar TARGET_DB=analytics \
  -f /path/to/load_orders.sql
```

Inside the script:

```sql
-- load_orders.sql
USE ${hivevar:TARGET_DB};

INSERT OVERWRITE TABLE monthly_summary
SELECT region, SUM(amount) AS total
FROM orders
WHERE order_date BETWEEN '${hivevar:START_DATE}' AND '${hivevar:END_DATE}'
GROUP BY region;
```

!!! tip "Quote date variables in SQL"
    Always wrap `${hivevar:date_var}` in single quotes inside SQL string contexts.

---

## :material-init: Initialisation File with `-i`

Run a shared setup script before starting the interactive prompt (or before `-f`):

```bash
# init.sql — shared across all scripts
SET spark.sql.adaptive.enabled = true;
SET spark.hadoop.hive.cli.print.header = true;

CREATE OR REPLACE TEMP VIEW fiscal_calendar AS
SELECT * FROM parquet.`/mnt/ref/fiscal_calendar/`;
```

```bash
spark-sql -i /path/to/init.sql -f /path/to/main_script.sql
```

---

## :material-pipe: Piping SQL via stdin

```bash
# Inline here-doc
spark-sql << 'EOF'
SELECT region, COUNT(*) AS cnt
FROM sales
GROUP BY region
ORDER BY cnt DESC;
EOF

# Pipe from echo
echo "SELECT current_date();" | spark-sql
```

---

## :material-check-all: Multi-Statement Scripts and Error Handling

By default `spark-sql` stops on the first error. Wrap risky statements in
`IF EXISTS` guards and use `CREATE OR REPLACE` to make scripts idempotent:

```sql
-- Safe script pattern
DROP TABLE IF EXISTS staging.temp_load;

CREATE TABLE staging.temp_load
USING DELTA
AS SELECT * FROM raw_orders WHERE order_date = '${hivevar:LOAD_DATE}';

-- Merge into target (idempotent)
MERGE INTO analytics.orders AS t
USING staging.temp_load AS s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

DROP TABLE IF EXISTS staging.temp_load;
```

---

## :material-clock-fast: Scheduling with cron / Airflow

```bash
#!/usr/bin/env bash
# run_daily_load.sh
set -euo pipefail

LOAD_DATE=$(date -d "yesterday" +%Y-%m-%d)

spark-sql \
  --master yarn \
  --executor-memory 8g \
  --num-executors 20 \
  --conf spark.sql.adaptive.enabled=true \
  --hivevar LOAD_DATE="${LOAD_DATE}" \
  --hivevar TARGET_DB=analytics \
  -f /opt/sql/daily_load.sql \
  2>&1 | tee "/var/log/spark-sql/daily_load_${LOAD_DATE}.log"
```

Run via cron:

```cron
0 2 * * * /opt/scripts/run_daily_load.sh
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Daily ETL job | `spark-sql -f` in a cron / Airflow task |
| Parameterised load | `--hivevar` for dates, schema names |
| Shared session setup | `-i init.sql` before the main script |
| CI/CD SQL smoke test | `spark-sql -e "SELECT 1"` in pipeline |
| One-off ad-hoc query | Interactive mode (`spark-sql`) |
