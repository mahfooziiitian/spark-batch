# :material-sigma: Aggregate Window Functions

Aggregate window functions apply the familiar aggregation operations (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT`) row-by-row over a sliding or bounded frame instead of collapsing all rows into one result.

---

## :material-pin: Syntax

```sql
{ SUM | AVG | MIN | MAX | COUNT | CUME_DIST }(expression)
OVER (
    [PARTITION BY partition_expression [, ...]]
    [ORDER BY    sort_expression [ASC | DESC] [, ...]]
    [{ ROWS | RANGE } BETWEEN frame_start AND frame_end]
)
```

---

## :material-magnify: Behavior

1. **Default frame**: when `ORDER BY` is present, the implicit frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (running aggregate up to and including the current row). Without `ORDER BY`, the frame covers the entire partition.
2. **`CUME_DIST` formula**: `number_of_rows_with_value_<=_current / total_rows_in_partition`. Result is always in `(0, 1]`.
3. **Running total vs partition total**: use `ORDER BY` with the default frame for a running total; omit `ORDER BY` (or use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`) for a static partition total on every row.
4. **Tie handling in `CUME_DIST`**: all rows that tie on the `ORDER BY` value receive the same `CUME_DIST` value equal to the highest rank among them.
5. **Combining patterns**: the same query can mix a running aggregate (with `ORDER BY`) and a partition aggregate (without `ORDER BY`) in separate `OVER` clauses — Spark evaluates each independently.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Alice', '2024-01-10', 300),
  ('North', 'Bob',   '2024-01-02', 150),
  ('North', 'Bob',   '2024-01-06', 300),
  ('South', 'Carol', '2024-01-03', 400),
  ('South', 'Carol', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);
```

### Example 1 — Running Total (Cumulative SUM)

```sql
SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales
ORDER BY region, sale_date;
-- Result:
-- | region | rep   | sale_date  | amount | running_total |
-- |--------|-------|------------|--------|---------------|
-- | North  | Alice | 2024-01-01 |    100 |           100 |
-- | North  | Bob   | 2024-01-02 |    150 |           250 |
-- | North  | Alice | 2024-01-05 |    200 |           450 |
-- | North  | Bob   | 2024-01-06 |    300 |           750 |
-- | North  | Alice | 2024-01-10 |    300 |          1050 |
-- | South  | Carol | 2024-01-03 |    400 |           400 |
-- | South  | Carol | 2024-01-07 |    500 |           900 |
```

### Example 2 — Partition Total (No ORDER BY)

```sql
SELECT
    region,
    rep,
    amount,
    SUM(amount) OVER (PARTITION BY region) AS region_total
FROM sales
ORDER BY region, sale_date;
-- Result:
-- | region | rep   | amount | region_total |
-- |--------|-------|--------|--------------|
-- | North  | Alice |    100 |         1050 |
-- | North  | Bob   |    150 |         1050 |
-- | North  | Alice |    200 |         1050 |
-- | North  | Bob   |    300 |         1050 |
-- | North  | Alice |    300 |         1050 |
-- | South  | Carol |    400 |          900 |
-- | South  | Carol |    500 |          900 |
```

### Example 3 — Moving Average (3-Row Centred)

```sql
SELECT
    region,
    rep,
    sale_date,
    amount,
    ROUND(AVG(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ), 2) AS moving_avg_3
FROM sales
ORDER BY region, sale_date;
-- Result:
-- | region | rep   | sale_date  | amount | moving_avg_3 |
-- |--------|-------|------------|--------|--------------|
-- | North  | Alice | 2024-01-01 |    100 |       125.00 |  -- only 2 rows in frame
-- | North  | Bob   | 2024-01-02 |    150 |       150.00 |
-- | North  | Alice | 2024-01-05 |    200 |       216.67 |
-- | North  | Bob   | 2024-01-06 |    300 |       266.67 |
-- | North  | Alice | 2024-01-10 |    300 |       300.00 |  -- only 2 rows in frame
-- | South  | Carol | 2024-01-03 |    400 |       450.00 |
-- | South  | Carol | 2024-01-07 |    500 |       450.00 |
```

### Example 4 — Cumulative Percentage Contribution

```sql
SELECT
    region,
    rep,
    sale_date,
    amount,
    ROUND(
        100.0 * SUM(amount) OVER (PARTITION BY region ORDER BY sale_date
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
              / SUM(amount) OVER (PARTITION BY region),
        1
    ) AS running_pct
FROM sales
ORDER BY region, sale_date;
-- Result:
-- | region | rep   | sale_date  | amount | running_pct |
-- |--------|-------|------------|--------|-------------|
-- | North  | Alice | 2024-01-01 |    100 |         9.5 |
-- | North  | Bob   | 2024-01-02 |    150 |        23.8 |
-- | North  | Alice | 2024-01-05 |    200 |        42.9 |
-- | North  | Bob   | 2024-01-06 |    300 |        71.4 |
-- | North  | Alice | 2024-01-10 |    300 |       100.0 |
-- | South  | Carol | 2024-01-03 |    400 |        44.4 |
-- | South  | Carol | 2024-01-07 |    500 |       100.0 |
```

### Example 5 — CUME_DIST per Region

```sql
SELECT
    region,
    rep,
    amount,
    ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY amount), 2) AS cume_dist
FROM sales
ORDER BY region, amount;
-- Result:
-- | region | rep   | amount | cume_dist |
-- |--------|-------|--------|-----------|
-- | North  | Alice |    100 |      0.20 |
-- | North  | Bob   |    150 |      0.40 |
-- | North  | Alice |    200 |      0.60 |
-- | North  | Alice |    300 |      1.00 |  -- tie: both 300 rows get 1.00
-- | North  | Bob   |    300 |      1.00 |
-- | South  | Carol |    400 |      0.50 |
-- | South  | Carol |    500 |      1.00 |
```

### Example 6 — MIN and MAX Across Full Partition

```sql
SELECT
    region,
    rep,
    amount,
    MIN(amount) OVER (PARTITION BY region) AS min_sale,
    MAX(amount) OVER (PARTITION BY region) AS max_sale
FROM sales
ORDER BY region, sale_date;
-- Result:
-- | region | rep   | amount | min_sale | max_sale |
-- |--------|-------|--------|----------|----------|
-- | North  | Alice |    100 |      100 |      300 |
-- | North  | Bob   |    150 |      100 |      300 |
-- | North  | Alice |    200 |      100 |      300 |
-- | North  | Bob   |    300 |      100 |      300 |
-- | North  | Alice |    300 |      100 |      300 |
-- | South  | Carol |    400 |      400 |      500 |
-- | South  | Carol |    500 |      400 |      500 |
```

---

## :material-table: Running Total vs Partition Total vs Moving Average

| Pattern | Row Varies? | Needs ORDER BY | Frame |
|---------|-------------|---------------|-------|
| Running total | Yes — grows with each row | Yes | `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| Partition total | No — same on every row | No | Full partition (implicit) |
| Moving average | Yes — centred on each row | Yes | `ROWS BETWEEN N PRECEDING AND N FOLLOWING` |

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Cumulative revenue by date | `SUM ... ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| Show each row alongside its group total | `SUM ... OVER (PARTITION BY ...)` with no `ORDER BY` |
| Smooth noisy time-series data | `AVG ... ROWS BETWEEN N PRECEDING AND N FOLLOWING` |
| Percentage of group total per row | Running SUM / partition SUM * 100 |
| Rank rows by distribution within a group | `CUME_DIST() OVER (PARTITION BY ... ORDER BY ...)` |
| Flag rows below/above the group min/max | `MIN`/`MAX` over full partition, then filter |
