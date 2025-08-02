# Ranking function

## Rank function

Computes the rank of a value in a group of values.

The result is one plus the number of rows preceding or equal to the current row in the ordering of the partition.

The values will produce gaps in the sequence.
RANK() skips numbers on ties, DENSE_RANK() doesn't.

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
  RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS ranked,
  DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS dense_ranked
FROM sales;
```

## dense_rank function

Computes the `rank` of a value in a group of values.

The result is one plus the previously assigned `rank` value.

Unlike the function rank, `dense_rank` will not produce gaps in the ranking sequence.

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
  RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS ranked,
  DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS dense_ranked
FROM sales;
```

## percent_rank

Computes the percentage ranking of a value in a group of values.

## row_number

Assigns a unique, sequential number to each row, starting with one, according to 
the ordering of rows within the window partition.

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
  ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date) AS row_num
FROM sales;

SELECT
    a,
    b,
    dense_rank() OVER(PARTITION BY a ORDER BY b),
    rank() OVER(PARTITION BY a ORDER BY b),
    row_number() OVER(PARTITION BY a ORDER BY b)
FROM 
    VALUES ('A1', 2),
    ('A1', 1), 
    ('A2', 3), 
    ('A1', 1) 
    tab(a, b);
```

## 🔀 NTILE(n) — Bucketing

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
  NTILE(3) OVER (PARTITION BY region ORDER BY amount DESC) AS bucket
FROM sales;
```

## 🔄 10. Use Window Functions in Subqueries

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
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY rep ORDER BY amount DESC) AS rn
  FROM sales
)
WHERE rn <= 2;
```
