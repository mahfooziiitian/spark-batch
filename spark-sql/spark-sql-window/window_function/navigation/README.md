# Navigational function

## 📦 FIRST_VALUE() and LAST_VALUE()

1. ⚠️ Default LAST_VALUE() is affected by current row — use explicit frame.

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
  FIRST_VALUE(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS first_amt,
  LAST_VALUE(amount) OVER (PARTITION BY rep ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt
FROM sales;
```

## Lead & Lag

Useful for calculating deltas between rows.

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
  LAG(amount, 1) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_sale,
  LEAD(amount, 1) OVER (PARTITION BY rep ORDER BY sale_date) AS next_sale
FROM sales;
```

### Calculate Difference Between Sales

Helps detect performance dips or jumps.

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
  amount - LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS diff_from_last
FROM sales;
```

## Nth_value

NTH_VALUE(column, n) returns the nth value in an ordered window frame (starting from 1).

Think of it as saying: “Give me the 3rd sale for each rep, ordered by date.”

```sql
NTH_VALUE(column, n) OVER (
  PARTITION BY ... 
  ORDER BY ... 
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
```

⚠️ ROWS clause is required if you want the full partition to be scanned (especially important for LAST_VALUE() and NTH_VALUE()).

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
  NTH_VALUE(amount, 2) OVER (
    PARTITION BY rep ORDER BY sale_date 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS second_sale
FROM sales;
```

### 3rd Sale per Region (Ordered by Amount)

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
  NTH_VALUE(amount, 3) OVER (
    PARTITION BY region ORDER BY amount 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS third_in_region
FROM sales;
```

### ✅ When to Use NTH_VALUE()

Use Case| Function
----|---
Find 3rd highest/lowest score| NTH_VALUE(..., 3)
Retain a specific rank row's value| NTH_VALUE(...)
Compare current row with a fixed row|Combine with LAG or NTH_VALUE

### ⚠️ Gotchas

1. If there are fewer than n rows in a partition → result is NULL
2. Always specify ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING for full-partition context
3. Works well with ordered metrics like sales, timestamps, scores
