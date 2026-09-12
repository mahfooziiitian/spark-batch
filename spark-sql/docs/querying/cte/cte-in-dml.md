# :material-database-edit-outline: CTE in DML Statements

A `WITH` block can feed DML just as easily as `SELECT`. In Spark 4.2, `INSERT` and `INSERT OVERWRITE` work directly on standard tables, while `MERGE`, `UPDATE`, and `DELETE` also accept leading CTEs but require a row-level-capable table provider at execution time.

### :material-animation-play: Interactive Visualization — CTE Feeding DML

<div id="viz-cte-dml-support" class="ts-viz"></div>

Use the buttons to compare portable `INSERT` patterns with row-level DML. The support badges reflect what was verified in PySpark 4.2: syntax is accepted broadly, but table capabilities determine whether `MERGE`, `UPDATE`, and `DELETE` can run.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
WITH prepared AS (
    SELECT ...
)
INSERT INTO target_table
SELECT * FROM prepared;
```

```sql
WITH prepared AS (
    SELECT ...
)
MERGE INTO target_table AS t
USING prepared AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET value = s.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);
```

```sql
WITH prepared AS (
    SELECT ...
)
UPDATE target_table
SET value = (
    SELECT MAX(p.value)
    FROM prepared AS p
    WHERE p.id = target_table.id
)
WHERE id IN (SELECT id FROM prepared);
```

```sql
WITH doomed AS (
    SELECT ...
)
DELETE FROM target_table
WHERE id IN (SELECT id FROM doomed);
```

______________________________________________________________________

## :material-information-outline: Verified Behavior in Spark 4.2

1. **`INSERT INTO` and `INSERT OVERWRITE` work directly** — both executed successfully against a managed Delta table on a Databricks SQL warehouse (`stg`) and against a managed Parquet table in PySpark 4.2.
2. **`MERGE`, `UPDATE`, and `DELETE` accept leading CTEs** — Spark 4.2 parsed these statements successfully, and all three executed successfully against a managed Delta table on Databricks.
3. **Execution depends on the target table provider** — running `MERGE`, `UPDATE`, or `DELETE` against a plain `USING parquet` table failed with `UNSUPPORTED_FEATURE.TABLE_OPERATION`. Databricks managed tables are Delta by default, so `MERGE`, `UPDATE`, and `DELETE` work without any extra configuration there.
4. **Portable `UPDATE` syntax does not require `FROM`** — `WITH ... UPDATE ... WHERE id IN (SELECT ...)` is accepted without a `FROM` clause; the `UPDATE ... FROM ...` form is not the portable choice here.
5. **The correlated subquery in `SET` must be provably single-row** — `SET value = (SELECT p.value FROM prepared AS p WHERE p.id = target_table.id)` fails on Databricks with `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY`, even when `p.id` is unique. Wrap the correlated column in an aggregate such as `MAX(p.value)` so the analyzer can guarantee at most one row per correlation.
6. **A CTE only changes readability, not transaction semantics** — atomic behavior comes from the table format and catalog implementation, not from `WITH` itself.

!!! note "Provider support matters"

    For row-level DML, think in two layers: first, does Spark accept the SQL shape? Second,
    does the target table implementation support the operation? A leading CTE solves only
    the first part.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### `INSERT`: clean and load

```sql
WITH cleaned AS (
    SELECT
        CAST(order_id AS BIGINT) AS order_id,
        TRIM(customer_name) AS customer_name,
        CAST(order_date AS DATE) AS order_date,
        CAST(amount AS DECIMAL(18, 2)) AS amount,
        UPPER(region) AS region
    FROM raw_orders
    WHERE order_id IS NOT NULL
      AND amount > 0
      AND order_date IS NOT NULL
)
INSERT INTO fact_orders
SELECT order_id, customer_name, order_date, amount, region
FROM cleaned;
```

### `INSERT OVERWRITE`: reload a derived table

```sql
WITH daily_totals AS (
    SELECT
        order_id,
        customer_id,
        SUM(amount) AS daily_total,
        COUNT(*) AS item_count
    FROM staging_orders
    WHERE order_date = CURRENT_DATE()
    GROUP BY order_id, customer_id
)
INSERT OVERWRITE TABLE daily_summary
SELECT order_id, customer_id, daily_total, item_count
FROM daily_totals;
```

### `MERGE`: deduplicate before matching

```sql
WITH deduped_source AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY updated_at DESC
            ) AS rn
        FROM staging_customers
    )
    WHERE rn = 1
)
MERGE INTO dim_customer AS t
USING deduped_source AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    name = s.name,
    email = s.email,
    city = s.city,
    updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    name,
    email,
    city,
    updated_at
) VALUES (
    s.customer_id,
    s.name,
    s.email,
    s.city,
    s.updated_at
);
```

### `UPDATE`: portable shape with CTE-driven filtering

```sql
WITH corrections AS (
    SELECT order_id, corrected_amount AS amount
    FROM order_corrections
    WHERE correction_date = CURRENT_DATE()
)
UPDATE fact_orders
SET amount = (
    SELECT MAX(c.amount)
    FROM corrections AS c
    WHERE c.order_id = fact_orders.order_id
)
WHERE order_id IN (SELECT order_id FROM corrections);
```

!!! note "Wrap the correlated column in an aggregate"

    Verified directly against a Databricks Delta table: `SET amount = (SELECT c.amount FROM corrections AS c WHERE c.order_id = fact_orders.order_id)` fails with
    `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY`,
    even when `order_id` is unique in `corrections`. Wrapping the selected column in `MAX()`
    (or another aggregate) satisfies the analyzer and the statement runs successfully.

### `DELETE`: purge rows selected by a CTE

```sql
WITH expired AS (
    SELECT user_id
    FROM user_sessions
    WHERE last_active < date_add(CURRENT_DATE(), -90)
)
DELETE FROM user_sessions
WHERE user_id IN (SELECT user_id FROM expired);
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use CTEs in DML

| Scenario                              | Pattern                  |
| ------------------------------------- | ------------------------ |
| Clean and cast rows before writing    | CTE + `INSERT INTO`      |
| Rebuild a derived target from scratch | CTE + `INSERT OVERWRITE` |
| Deduplicate a source before `MERGE`   | Window CTE + `MERGE`     |
| Apply a filtered set of corrections   | CTE + `UPDATE`           |
| Delete rows chosen by complex logic   | CTE + `DELETE`           |

!!! tip "Push complexity above the DML body"

    Keep the write clause focused on matching and actions. Put cleansing, deduplication,
    and enrichment in the CTE so the DML statement reads like business logic instead of plumbing.
