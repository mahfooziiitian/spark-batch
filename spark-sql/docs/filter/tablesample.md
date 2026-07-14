# :material-table-arrow-right: TABLESAMPLE

`TABLESAMPLE` returns a random subset of rows from a table — useful for exploration, testing, and profiling without scanning the full dataset.

---

## :material-code-tags: Syntax

```sql
-- Percentage-based sample
SELECT * FROM table TABLESAMPLE (N PERCENT);

-- Fixed row count sample
SELECT * FROM table TABLESAMPLE (N ROWS);

-- Reproducible sample with a seed
SELECT * FROM table TABLESAMPLE (N PERCENT) REPEATABLE (seed);

-- Bucket sampling (deterministic by hash bucket)
SELECT * FROM table TABLESAMPLE (BUCKET m OUT OF n ON col);
```

| Parameter | Description |
|-----------|-------------|
| `N PERCENT` | Approximate percentage of rows to return (0–100) |
| `N ROWS` | Exact number of rows to return |
| `REPEATABLE (seed)` | Integer seed for reproducible results |
| `BUCKET m OUT OF n ON col` | Return bucket `m` of `n` hash-buckets on column `col` |

---

## :material-information-outline: Behavior

1. **PERCENT is approximate** — Spark uses Bernoulli sampling per partition; the actual row count may vary slightly from the target percentage.
2. **ROWS is exact** — `TABLESAMPLE (N ROWS)` scans partitions and stops after `N` rows; the result set size is guaranteed.
3. **REPEATABLE seed** — identical seed + identical data produces the same sample; adding or removing partitions may change results.
4. **Bucket sampling is deterministic** — rows are assigned buckets by hashing `col`; `BUCKET 1 OUT OF 10 ON id` always selects the same 10% of rows for a given dataset.
5. **No pushdown** — `TABLESAMPLE` does not reduce the amount of data read from storage; it discards rows after scanning. Use partition pruning (`WHERE`) first to reduce I/O.

---

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
-- Exactly 1000 rows — useful for unit tests and pipeline validation
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

### :material-numeric-5-circle: Bucket sampling — deterministic 10%

```sql
-- Always returns the same rows for a given dataset version
SELECT *
FROM transactions
TABLESAMPLE (BUCKET 1 OUT OF 10 ON transaction_id);
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

### :material-numeric-7-circle: Combine with WHERE for partition-pruned sampling

```sql
-- Prune to recent data first, then sample — reduces I/O significantly
SELECT *
FROM orders
WHERE order_date >= '2024-01-01'     -- partition prune first
TABLESAMPLE (2 PERCENT) REPEATABLE (7);
```

---

## :material-swap-horizontal: Sampling Methods Compared

| Method | Deterministic | Row Count | When to Use |
|--------|--------------|-----------|-------------|
| `TABLESAMPLE (N PERCENT)` | No | Approximate | Quick exploration |
| `TABLESAMPLE (N PERCENT) REPEATABLE (seed)` | Yes (same data) | Approximate | Reproducible experiments |
| `TABLESAMPLE (N ROWS)` | No | Exact | Fixed-size test datasets |
| `TABLESAMPLE (BUCKET m OUT OF n ON col)` | Yes | Exact fraction | Consistent cross-run splits |
| `ORDER BY RAND() LIMIT N` | No | Exact | Exact N with global sort — expensive |

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended |
|----------|-------------|
| Data exploration on large tables | `TABLESAMPLE (5 PERCENT)` |
| Repeatable ML train/test splits | `TABLESAMPLE (N PERCENT) REPEATABLE (seed)` |
| Pipeline smoke testing | `TABLESAMPLE (1000 ROWS)` |
| Approximate statistics (profiling) | `TABLESAMPLE (1 PERCENT)` + aggregation |
| Consistent sampling across multiple queries | `TABLESAMPLE (BUCKET 1 OUT OF 10 ON id)` |
| Exact random N rows | `ORDER BY RAND() LIMIT N` (use only on small tables) |

---

## :material-shield-outline: Performance Tips

!!! warning "TABLESAMPLE does not prune partitions"
    `TABLESAMPLE` still scans all partitions and filters afterwards. Always push
    `WHERE` predicates on partition columns **before** the sample clause to reduce
    data read from storage.

```sql
-- Efficient: partition prune first, then sample
SELECT * FROM events
WHERE event_date = '2024-06-01'
TABLESAMPLE (10 PERCENT) REPEATABLE (1);

-- Expensive: full table scan, then 10% kept
SELECT * FROM events
TABLESAMPLE (10 PERCENT) REPEATABLE (1);
```

!!! tip "Use `TABLESAMPLE` in ETL validation"
    Before running a full production pipeline, validate transformations on a sample:
    ```sql
    SELECT * FROM source_table TABLESAMPLE (1000 ROWS)
    ```
    This catches schema mismatches and logic errors without the cost of a full run.
