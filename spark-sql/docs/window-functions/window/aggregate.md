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

### Dataset

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 'Electronics', '2024-01-01', 100),
  ('North', 'Alice', 'Electronics', '2024-01-05', 200),
  ('North', 'Alice', 'Clothing',    '2024-01-10', 300),
  ('North', 'Bob',   'Electronics', '2024-01-02', 150),
  ('North', 'Bob',   'Clothing',    '2024-01-06', 300),
  ('North', 'Bob',   'Electronics', '2024-01-12', 250),
  ('South', 'Carol', 'Electronics', '2024-01-03', 400),
  ('South', 'Carol', 'Clothing',    '2024-01-07', 500),
  ('South', 'Dave',  'Electronics', '2024-01-04', 350),
  ('South', 'Dave',  'Clothing',    '2024-01-09', 275)
AS sales(region, rep, category, sale_date, amount);
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
```

| region | rep   | sale_date  | amount | running_total |
|--------|-------|------------|-------:|:-------------:|
| North  | Alice | 2024-01-01 |    100 | 100 |
| North  | Bob   | 2024-01-02 |    150 | 250 |
| North  | Alice | 2024-01-05 |    200 | 450 |
| North  | Bob   | 2024-01-06 |    300 | 750 |
| North  | Alice | 2024-01-10 |    300 | 1050 |
| North  | Bob   | 2024-01-12 |    250 | 1300 |
| South  | Carol | 2024-01-03 |    400 | 400 |
| South  | Dave  | 2024-01-04 |    350 | 750 |
| South  | Carol | 2024-01-07 |    500 | 1250 |
| South  | Dave  | 2024-01-09 |    275 | 1525 |

### Example 2 — Partition Total (No ORDER BY)

```sql
SELECT
    region,
    rep,
    amount,
    SUM(amount) OVER (PARTITION BY region) AS region_total,
    ROUND(100.0 * amount / SUM(amount) OVER (PARTITION BY region), 1) AS pct_of_region
FROM sales
ORDER BY region, sale_date;
```

| region | rep   | amount | region_total | pct_of_region |
|--------|-------|-------:|:------------:|:-------------:|
| North  | Alice |    100 | 1300 | 7.7 |
| North  | Bob   |    150 | 1300 | 11.5 |
| North  | Alice |    200 | 1300 | 15.4 |
| North  | Bob   |    300 | 1300 | 23.1 |
| North  | Alice |    300 | 1300 | 23.1 |
| North  | Bob   |    250 | 1300 | 19.2 |
| South  | Carol |    400 | 1525 | 26.2 |
| South  | Dave  |    350 | 1525 | 23.0 |
| South  | Carol |    500 | 1525 | 32.8 |
| South  | Dave  |    275 | 1525 | 18.0 |

### Example 3 — Moving Average (3-Row Window)

```sql
SELECT
    region,
    sale_date,
    amount,
    ROUND(AVG(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ), 0) AS moving_avg_3
FROM sales
ORDER BY region, sale_date;
```

| region | sale_date  | amount | moving_avg_3 |
|--------|------------|-------:|:------------:|
| North  | 2024-01-01 |    100 | 125 |
| North  | 2024-01-02 |    150 | 150 |
| North  | 2024-01-05 |    200 | 217 |
| North  | 2024-01-06 |    300 | 267 |
| North  | 2024-01-10 |    300 | 283 |
| North  | 2024-01-12 |    250 | 275 |
| South  | 2024-01-03 |    400 | 375 |
| South  | 2024-01-04 |    350 | 417 |
| South  | 2024-01-07 |    500 | 375 |
| South  | 2024-01-09 |    275 | 388 |

!!! note "Boundary rows"
    At partition edges, the frame contains fewer rows (2 instead of 3).
    The average is still correct — Spark divides by the actual row count in the frame.

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
```

| region | rep   | sale_date  | amount | running_pct |
|--------|-------|------------|-------:|:-----------:|
| North  | Alice | 2024-01-01 |    100 | 7.7 |
| North  | Bob   | 2024-01-02 |    150 | 19.2 |
| North  | Alice | 2024-01-05 |    200 | 34.6 |
| North  | Bob   | 2024-01-06 |    300 | 57.7 |
| North  | Alice | 2024-01-10 |    300 | 80.8 |
| North  | Bob   | 2024-01-12 |    250 | 100.0 |
| South  | Carol | 2024-01-03 |    400 | 26.2 |
| South  | Dave  | 2024-01-04 |    350 | 49.2 |
| South  | Carol | 2024-01-07 |    500 | 82.0 |
| South  | Dave  | 2024-01-09 |    275 | 100.0 |

### Example 5 — COUNT Variations

```sql
SELECT
    region,
    rep,
    sale_date,
    COUNT(*) OVER (PARTITION BY region)                                 AS total_rows,
    COUNT(*) OVER (PARTITION BY region ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS running_count,
    COUNT(*) OVER (PARTITION BY region, rep)                             AS rep_sales_count
FROM sales
ORDER BY region, sale_date;
```

| region | rep   | sale_date  | total_rows | running_count | rep_sales_count |
|--------|-------|------------|:----------:|:-------------:|:---------------:|
| North  | Alice | 2024-01-01 | 6 | 1 | 3 |
| North  | Bob   | 2024-01-02 | 6 | 2 | 3 |
| North  | Alice | 2024-01-05 | 6 | 3 | 3 |
| North  | Bob   | 2024-01-06 | 6 | 4 | 3 |
| North  | Alice | 2024-01-10 | 6 | 5 | 3 |
| North  | Bob   | 2024-01-12 | 6 | 6 | 3 |
| South  | Carol | 2024-01-03 | 4 | 1 | 2 |
| South  | Dave  | 2024-01-04 | 4 | 2 | 2 |
| South  | Carol | 2024-01-07 | 4 | 3 | 2 |
| South  | Dave  | 2024-01-09 | 4 | 4 | 2 |

### Example 6 — CUME_DIST per Region

```sql
SELECT
    region,
    rep,
    amount,
    ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY amount), 2) AS cume_dist
FROM sales
ORDER BY region, amount;
```

| region | rep   | amount | cume_dist |
|--------|-------|-------:|:---------:|
| North  | Alice |    100 | 0.17 |
| North  | Bob   |    150 | 0.33 |
| North  | Alice |    200 | 0.50 |
| North  | Bob   |    250 | 0.67 |
| North  | Alice |    300 | 1.00 |
| North  | Bob   |    300 | 1.00 |
| South  | Dave  |    275 | 0.25 |
| South  | Dave  |    350 | 0.50 |
| South  | Carol |    400 | 0.75 |
| South  | Carol |    500 | 1.00 |

!!! tip "Tie handling"
    Both 300-amount rows in North get `cume_dist = 1.00` because all rows
    with value ≤ 300 comprise 100% of the partition.

### Example 7 — MIN / MAX with Running Extremes

```sql
SELECT
    region,
    sale_date,
    amount,
    MIN(amount) OVER (PARTITION BY region) AS region_min,
    MAX(amount) OVER (PARTITION BY region) AS region_max,
    MIN(amount) OVER (PARTITION BY region ORDER BY sale_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min,
    MAX(amount) OVER (PARTITION BY region ORDER BY sale_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max
FROM sales
ORDER BY region, sale_date;
```

| region | sale_date  | amount | region_min | region_max | running_min | running_max |
|--------|------------|-------:|:----------:|:----------:|:-----------:|:-----------:|
| North  | 2024-01-01 |    100 | 100 | 300 | 100 | 100 |
| North  | 2024-01-02 |    150 | 100 | 300 | 100 | 150 |
| North  | 2024-01-05 |    200 | 100 | 300 | 100 | 200 |
| North  | 2024-01-06 |    300 | 100 | 300 | 100 | 300 |
| North  | 2024-01-10 |    300 | 100 | 300 | 100 | 300 |
| North  | 2024-01-12 |    250 | 100 | 300 | 100 | 300 |
| South  | 2024-01-03 |    400 | 275 | 500 | 400 | 400 |
| South  | 2024-01-04 |    350 | 275 | 500 | 350 | 400 |
| South  | 2024-01-07 |    500 | 275 | 500 | 350 | 500 |
| South  | 2024-01-09 |    275 | 275 | 500 | 275 | 500 |

### Example 8 — Multi-Level Aggregation in One Query

Combine rep-level, category-level, and region-level aggregates without multiple passes:

```sql
SELECT
    region,
    rep,
    category,
    amount,
    SUM(amount) OVER (PARTITION BY region, rep)      AS rep_total,
    SUM(amount) OVER (PARTITION BY region, category) AS category_total,
    SUM(amount) OVER (PARTITION BY region)           AS region_total,
    SUM(amount) OVER ()                              AS grand_total
FROM sales
ORDER BY region, rep, category;
```

| region | rep   | category    | amount | rep_total | category_total | region_total | grand_total |
|--------|-------|-------------|-------:|:---------:|:--------------:|:------------:|:-----------:|
| North  | Alice | Clothing    |    300 | 600 | 600 | 1300 | 2825 |
| North  | Alice | Electronics |    100 | 600 | 700 | 1300 | 2825 |
| North  | Alice | Electronics |    200 | 600 | 700 | 1300 | 2825 |
| North  | Bob   | Clothing    |    300 | 700 | 600 | 1300 | 2825 |
| North  | Bob   | Electronics |    150 | 700 | 700 | 1300 | 2825 |
| North  | Bob   | Electronics |    250 | 700 | 700 | 1300 | 2825 |
| South  | Carol | Clothing    |    500 | 900 | 775 | 1525 | 2825 |
| South  | Carol | Electronics |    400 | 900 | 750 | 1525 | 2825 |
| South  | Dave  | Clothing    |    275 | 625 | 775 | 1525 | 2825 |
| South  | Dave  | Electronics |    350 | 625 | 750 | 1525 | 2825 |

!!! tip "Performance"
    Multiple `OVER` specs with the same `PARTITION BY` share a single shuffle stage.
    In this example, `PARTITION BY region, rep` and `PARTITION BY region, category`
    use different partitioning — that's 3 shuffles total (rep, category, region + grand).

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
