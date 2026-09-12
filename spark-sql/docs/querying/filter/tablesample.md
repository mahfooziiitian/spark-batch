# :material-table-arrow-right: TABLESAMPLE

`TABLESAMPLE` returns a subset of rows from a table — useful for exploration, testing, and profiling without scanning every result row.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
-- Percentage-based sample
SELECT * FROM table TABLESAMPLE (N PERCENT);

-- Percentage sample with a seed
SELECT * FROM table TABLESAMPLE (N PERCENT) REPEATABLE (seed);

-- Fixed row-count sample
SELECT * FROM table TABLESAMPLE (N ROWS);

-- Fixed row-count sample with a seed
SELECT * FROM table TABLESAMPLE (N ROWS) REPEATABLE (seed);

-- Fractional bucket syntax supported by Spark 4.2
SELECT * FROM table TABLESAMPLE (BUCKET m OUT OF n);

-- Seeded bucket syntax
SELECT * FROM table TABLESAMPLE (BUCKET m OUT OF n) REPEATABLE (seed);
```

| Parameter           | Description                                                              |
| ------------------- | ------------------------------------------------------------------------ |
| `N PERCENT`         | Approximate percentage of rows to return (0–100)                         |
| `N ROWS`            | Exact number of rows to return                                           |
| `REPEATABLE (seed)` | Integer seed for reproducible Bernoulli samples; accepted for `ROWS` too |
| `BUCKET m OUT OF n` | Fractional Bernoulli sample using `m / n`                                |

### :material-animation-play: Interactive Visualization — Sampling Modes

<div id="viz-filter-tablesample" class="ts-viz"></div>

Switch among percentage, row-count, and bucket forms to compare stability and row-count guarantees. The labels reflect Spark 4.2 execution plans and repeated-query checks.

______________________________________________________________________

## :material-information-outline: Behavior

1. **PERCENT is approximate** — Spark uses Bernoulli sampling per partition; the actual row count may vary slightly from the target percentage.
2. **ROWS is exact but not random** — in Spark 4.2 and on Databricks SQL, `TABLESAMPLE (N ROWS)` returns exactly `N` rows when available, but the rows come from storage/input order rather than true random selection. `REPEATABLE` is accepted after `ROWS`, yet verification on a live Databricks SQL warehouse returned the same 10-row slice for different seeds.
3. **`REPEATABLE` makes Bernoulli sampling reproducible** — identical seed + identical input returns the same `PERCENT` sample, and also stabilises the supported `BUCKET m OUT OF n` form.
4. **Spark 4.2 does not support `ON col` bucket syntax** — `TABLESAMPLE (BUCKET m OUT OF n ON col)` is also rejected on Databricks SQL, which returns: `TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported.`
5. **`BUCKET m OUT OF n` is not hash-bucket sampling here** — Spark 4.2 plans it as a Bernoulli sample with fraction `m / n`, so unseeded runs vary.
6. **Clause order matters** — `TABLESAMPLE` is part of the `FROM` relation, so write `FROM table TABLESAMPLE (...) WHERE ...`, not the other way around.

______________________________________________________________________

## :material-information-outline: Verified Behavior on Databricks SQL

1. **`PERCENT` and `BUCKET ... REPEATABLE` were reproducible** — repeated queries against a real managed Delta table on a Databricks SQL warehouse (`stg`) returned identical row sets for the same seed and different row sets for different seeds.
2. **`ROWS` stayed exact and input-stable** — `TABLESAMPLE (10 ROWS)` always returned 10 rows, but changing `REPEATABLE` from 1 to 99 did not change which 10 rows were returned. Treat `ROWS` as a fixed-size slice, not a random sample.
3. **Databricks still pushed filters into Delta scans** — `EXPLAIN FORMATTED` for `SELECT * FROM partitioned_table TABLESAMPLE (...) WHERE event_date = ...` showed `PartitionFilters` on the Photon scan. Use a CTE to make the sampled population explicit or to reuse the filtered relation, not because Databricks always blocks pushdown after `TABLESAMPLE`.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### :material-numeric-1-circle: Quick 5% exploratory sample

```sql
SELECT *
FROM orders
TABLESAMPLE (5 PERCENT);
-- Returns ~5% of rows — row count varies each run
```

### :material-numeric-2-circle: Fixed-count sample for testing

```sql
-- Exactly 1000 rows when the table has at least 1000 rows
-- Useful for smoke tests; not a random sample
SELECT *
FROM large_events
TABLESAMPLE (1000 ROWS);
```

### :material-numeric-3-circle: Reproducible sample for ML feature engineering

```sql
-- Same 10% every run (seed = 42)
SELECT customer_id, features, label
FROM training_data
TABLESAMPLE (10 PERCENT) REPEATABLE (42);
```

### :material-numeric-4-circle: Train / test split

```sql
-- 80% training set
CREATE OR REPLACE TEMP VIEW train AS
SELECT * FROM ml_dataset TABLESAMPLE (80 PERCENT) REPEATABLE (1);

-- Complement: rows NOT in the training sample (approximate)
CREATE OR REPLACE TEMP VIEW test AS
SELECT d.*
FROM ml_dataset AS d
LEFT ANTI JOIN train AS t ON d.id = t.id;
```

### :material-numeric-5-circle: Bucket syntax with a reproducible seed

```sql
-- In Spark 4.2 this is a Bernoulli-style 10% sample, not hash-bucket routing
-- Add REPEATABLE if you need the same sample each run
SELECT *
FROM transactions
TABLESAMPLE (BUCKET 1 OUT OF 10) REPEATABLE (7);
```

### :material-numeric-6-circle: Sample before aggregation (approx statistics)

```sql
-- Approximate average on 1% of data — fast profiling
SELECT
    region,
    COUNT(*)        AS sampled_rows,
    AVG(amount)     AS approx_avg_amount,
    STDDEV(amount)  AS approx_stddev
FROM orders
TABLESAMPLE (1 PERCENT) REPEATABLE (99)
GROUP BY region
ORDER BY approx_avg_amount DESC;
```

### :material-numeric-7-circle: Filter first, then sample via a CTE

```sql
-- Materialise the filtered relation first, then sample that smaller input
WITH recent_orders AS (
    SELECT *
    FROM orders
    WHERE order_date >= '2024-01-01'
)
SELECT *
FROM recent_orders
TABLESAMPLE (2 PERCENT) REPEATABLE (7);
```

______________________________________________________________________

## :material-swap-horizontal: Sampling Methods Compared

| Method                                      | Deterministic   | Row Count   | When to Use                             |
| ------------------------------------------- | --------------- | ----------- | --------------------------------------- |
| `TABLESAMPLE (N PERCENT)`                   | No              | Approximate | Quick exploration                       |
| `TABLESAMPLE (N PERCENT) REPEATABLE (seed)` | Yes (same data) | Approximate | Reproducible experiments                |
| `TABLESAMPLE (N ROWS)`                      | Input-stable    | Exact       | Fixed-size smoke tests                  |
| `TABLESAMPLE (BUCKET m OUT OF n)`           | No              | Approximate | Fractional sample with alternate syntax |
| `ORDER BY RAND() LIMIT N`                   | No              | Exact       | Exact N with global sort — expensive    |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                             | Recommended                                          |
| ------------------------------------ | ---------------------------------------------------- |
| Data exploration on large tables     | `TABLESAMPLE (5 PERCENT)`                            |
| Repeatable ML train/test splits      | `TABLESAMPLE (N PERCENT) REPEATABLE (seed)`          |
| Pipeline smoke testing               | `TABLESAMPLE (1000 ROWS)`                            |
| Approximate statistics (profiling)   | `TABLESAMPLE (1 PERCENT)` + aggregation              |
| Reproducible alternate sample syntax | `TABLESAMPLE (BUCKET 1 OUT OF 10) REPEATABLE (seed)` |
| Exact random N rows                  | `ORDER BY RAND() LIMIT N` (use only on small tables) |

______________________________________________________________________

## :material-shield-outline: Performance Tips

!!! note "Databricks can still prune pushdown-friendly filters"

    On a live Databricks SQL warehouse, `EXPLAIN FORMATTED` showed `PartitionFilters`
    on the Delta scan even when the predicate was written after `TABLESAMPLE`.
    Clause order still matters syntactically, but Photon can often push deterministic
    filters into the scan before sampling. Use a CTE when you want to make the sampled
    population explicit or reuse the filtered relation.

```sql
-- Explicit: define the population first, then sample it
WITH day_events AS (
    SELECT * FROM events WHERE event_date = '2024-06-01'
)
SELECT * FROM day_events TABLESAMPLE (10 PERCENT) REPEATABLE (1);

-- Also valid on Databricks; Delta may still push the filter into the scan
SELECT * FROM events TABLESAMPLE (10 PERCENT) REPEATABLE (1)
WHERE event_date = '2024-06-01';
```

!!! tip "Use `TABLESAMPLE` in ETL validation"

    Before running a full production pipeline, validate transformations on a sample:

    ```sql
    SELECT * FROM source_table TABLESAMPLE (1000 ROWS)
    ```

    This catches schema mismatches and logic errors without the cost of a full run.

<script src="../../../assets/js/querying-filter-viz.js"></script>
