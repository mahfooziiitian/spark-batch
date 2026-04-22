# :material-ruler: RANGE Frame

`RANGE` frames define the window using a **value-based offset** from the current row's `ORDER BY` value, not a physical row count. All rows whose `ORDER BY` value falls within the range are included in the frame.

---

## :material-pin: Syntax

```sql
RANGE BETWEEN range_start AND range_end
```

Where each boundary is:

```sql
UNBOUNDED PRECEDING
N PRECEDING              -- numeric distance
INTERVAL N UNITS PRECEDING  -- date/timestamp distance
CURRENT ROW
N FOLLOWING
INTERVAL N UNITS FOLLOWING
UNBOUNDED FOLLOWING
```

---

## :material-magnify: Behavior

1. **Tied ORDER BY values**: all rows sharing the same `ORDER BY` value are always included in the same `RANGE` frame — they are never split.
2. **Numeric or date ORDER BY only**: `RANGE` offsets require the `ORDER BY` column to be numeric or date/timestamp. String or boolean columns are not supported with offset boundaries.
3. **INTERVAL for dates**: use `INTERVAL N DAYS` (or `HOURS`, `MONTHS`, etc.) when the `ORDER BY` column is a `DATE` or `TIMESTAMP`.
4. **Multi-column ORDER BY not supported with offsets**: `RANGE BETWEEN N PRECEDING AND CURRENT ROW` requires exactly one column in `ORDER BY`. Use `ROWS` when multiple sort columns are needed.

---

## :material-flask-outline: Examples

```sql
CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT * FROM VALUES
  (CAST('2024-01-01' AS DATE), 100),
  (CAST('2024-01-03' AS DATE), 200),
  (CAST('2024-01-05' AS DATE), 150),
  (CAST('2024-01-07' AS DATE), 300),
  (CAST('2024-01-10' AS DATE), 250)
AS daily_sales(sale_date, amount);
```

### Example 1 — 7-Day Rolling Total

```sql
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (
        ORDER BY CAST(sale_date AS TIMESTAMP)
        RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    ) AS rolling_7d_total
FROM daily_sales
ORDER BY sale_date;
-- Result:
-- | sale_date  | amount | rolling_7d_total |
-- |------------|--------|------------------|
-- | 2024-01-01 |    100 |              100 |
-- | 2024-01-03 |    200 |              300 |
-- | 2024-01-05 |    150 |              450 |
-- | 2024-01-07 |    300 |              650 |  -- 2024-01-01 is 6 days before, included
-- | 2024-01-10 |    250 |              700 |  -- 2024-01-03 falls outside 6-day window
```

### Example 2 — Numeric Amount Range

```sql
CREATE OR REPLACE TEMP VIEW scored AS
SELECT * FROM VALUES
  ('Alice', 100),
  ('Bob',   150),
  ('Carol', 200),
  ('Dave',  280),
  ('Eve',   320)
AS scored(name, score);

SELECT
    name,
    score,
    COUNT(*) OVER (
        ORDER BY score
        RANGE BETWEEN 100 PRECEDING AND CURRENT ROW
    ) AS peers_within_100
FROM scored
ORDER BY score;
-- Result:
-- | name  | score | peers_within_100 |
-- |-------|-------|------------------|
-- | Alice |   100 |                1 |  -- no scores in [0, 100] below
-- | Bob   |   150 |                2 |  -- Alice (100) within 100 below
-- | Carol |   200 |                2 |  -- Bob (150) within 100, Alice outside
-- | Dave  |   280 |                2 |  -- Carol (200) within 100, Bob outside
-- | Eve   |   320 |                2 |  -- Dave (280) within 100, Carol outside
```

### Example 3 — Invalid: String Column with RANGE Offset

```sql
-- ERROR: RANGE with a string ORDER BY column and a numeric offset is not supported.
-- The following query will fail at runtime:
SELECT
    name,
    score,
    SUM(score) OVER (
        ORDER BY name          -- string column
        RANGE BETWEEN 1 PRECEDING AND CURRENT ROW  -- numeric offset on string: invalid
    ) AS bad_range
FROM scored;
-- SparkException: Window frame RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
-- requires the ORDER BY expression to be of a numeric type or date/timestamp type.
-- Use ROWS instead when ORDER BY is a string column.
```

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Rolling N-day revenue window | `RANGE BETWEEN INTERVAL N DAYS PRECEDING AND CURRENT ROW` |
| Include all rows within a numeric tolerance band | `RANGE BETWEEN N PRECEDING AND CURRENT ROW` |
| Running total where tied dates must show the same cumulative sum | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| Per-row window requiring physical row counts | Use `ROWS` instead |
