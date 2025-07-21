# Aggregate Functions

## Syntax

```text

MAX | MIN | COUNT | SUM | AVG
```

## Examples

### Running total (cumulative sum)

Definition: Adds up values row-by-row in order, resetting for each partition (e.g. region or rep).

1. 📌 Increments row by row. Great for charts, balances, rankings.
2. Use case: Charting or tracking performance over time.

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 100),
  ('North', 'Bob', 200),
  ('North', 'Alice', 300),
  ('South', 'Alice', 400),
  ('South', 'Bob', 500),
  ('South', 'Bob', 600)
AS sales(region, rep, amount);
SELECT *,
  SUM(amount) OVER (PARTITION BY region ORDER BY amount) AS running_total
FROM sales;
```

Equivalent SQL

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 100),
  ('North', 'Bob', 200),
  ('North', 'Alice', 300),
  ('South', 'Alice', 400),
  ('South', 'Bob', 500),
  ('South', 'Bob', 600)
AS sales(region, rep, amount);
SELECT *,
  SUM(amount) OVER (
    PARTITION BY region
    ORDER BY amount
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM sales;
```

### Cumulative % Contribution

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 100),
  ('North', 'Bob', 200),
  ('North', 'Alice', 300),
  ('South', 'Alice', 400),
  ('South', 'Bob', 500),
  ('South', 'Bob', 600)
AS sales(region, rep, amount);
SELECT *,
  SUM(amount) OVER (PARTITION BY region ORDER BY amount) * 1.0 /
  SUM(amount) OVER (PARTITION BY region) AS pct_of_total
FROM sales;
```

### Total per partition

Definition: Computes a single total for each partition group, and repeats that same value on each row in the group.

📌 Same total on every row in that partition. Useful for computing % of total, group stats.

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 100),
  ('North', 'Bob', 200),
  ('North', 'Alice', 300),
  ('South', 'Alice', 400),
  ('South', 'Bob', 500),
  ('South', 'Bob', 600)
AS sales(region, rep, amount);
SELECT *,
  SUM(amount) OVER (PARTITION BY region) AS region_total
FROM sales;
```

## Moving average - AVG() OVER

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Alice', '2024-01-10', 300),
  ('North', 'Bob',   '2024-01-02', 150),
  ('North', 'Bob',   '2024-01-06', 250),
  ('South', 'Alice', '2024-01-03', 400),
  ('South', 'Alice', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);
SELECT *,
  AVG(amount) OVER (
    PARTITION BY rep ORDER BY sale_date
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  ) AS moving_avg
FROM sales;
```

## cume_dist

`📖 Definition`: CUME_DIST() returns the cumulative distribution of a row within a partition — i.e., the relative rank of the row compared to the total number of rows.

It calculates:

```text
(Number of rows with values ≤ current row) / (Total number of rows in partition)
```

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Alice', '2024-01-10', 300),
  ('North', 'Bob',   '2024-01-02', 150),
  ('North', 'Bob',   '2024-01-06', 250),
  ('South', 'Alice', '2024-01-03', 400),
  ('South', 'Alice', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);
SELECT *,
  CUME_DIST() OVER (ORDER BY amount) AS cume_dist
FROM sales;
```

## 🔍 Summary Table

Feature |Running Total |Total per Partition
---|---|---
Varies by row? |✅ Yes, grows cumulatively |❌ No, same for all in partition
Requires ORDER BY? |✅ Yes |❌ No (optional)
Use case |Progress tracking, time series |Percent of total, comparisons
Example Output |100 → 300 → 600 → … |1500 → 1500 → 1500 → …

## Combing both

✔️ Get row-by-row and overall metrics side-by-side.

```sql

SELECT *,
  SUM(amount) OVER (PARTITION BY region ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  SUM(amount) OVER (PARTITION BY region) AS total_by_region
FROM sales;
```
