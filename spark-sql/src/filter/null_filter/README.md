# Null handling in filter

Handling NULLs in filters is a critical aspect of writing correct Spark SQL queries — because Spark SQL follows SQL's 3-valued logic `(TRUE, FALSE, NULL)`, which can cause unexpected results if you're not careful.

## 📌 What to Know About NULLs in Spark SQL Filtering

🔸 In filters like `col != 'value'`, NULLs do not match anything — even inequality checks.
🔸 Comparisons involving `NULL return NULL`, not `TRUE or FALSE`.
🔸 You must use `IS NULL / IS NOT NULL` for explicit NULL filtering.

## ✅ 1. Detect NULL values explicitly

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);

SELECT * FROM null_demo
WHERE amount IS NULL;
```

✔️ Returns rows where the column is null.

## ✅ 2. Exclude NULLs

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);
SELECT * FROM null_demo
WHERE product IS NOT NULL;
```

## Problem with inequality filters and NULLs

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);
-- Won't return NULLs or match them
SELECT * FROM null_demo
WHERE product != 'A'; 
```

Rows where column_name IS NULL will be excluded silently because:

1. 'NULL != abc' evaluates to NULL
2. NULL is not TRUE, so it's filtered out

✅ To include NULLs in logic:

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);
-- Won't return NULLs or match them
SELECT * FROM null_demo
WHERE product != 'A' OR product IS NULL;
```

## ✅ 4. Using COALESCE() to replace NULLs in filters

```sql
-- Replace NULL with 'unknown' before comparing
SELECT * FROM table_name
WHERE COALESCE(column_name, 'unknown') = 'unknown';
```

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);
-- Treat NULLs in amount as 0
SELECT * FROM null_demo
WHERE COALESCE(amount, 0) > 100;
```

## ✅ 5. Use CASE WHEN with NULL-safe logic

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);
-- Find rows where product is NULL
SELECT id, name,
  CASE
    WHEN amount IS NULL THEN 'Missing'
    WHEN amount > 300 THEN 'High'
    ELSE 'Low'
  END AS amount_category
FROM null_demo;
```

## ✅ 6. NULL-safe equality (<=>) — Spark-specific

✔️ Works like product IS NULL, but allows comparison in join or dynamic condition.

```sql
CREATE OR REPLACE TEMP VIEW null_demo AS
SELECT * FROM VALUES
  (1, 'Alice', 100, 'A'),
  (2, 'Bob', NULL, 'B'),
  (3, NULL, 300, NULL),
  (4, 'Dana', 400, 'A'),
  (5, 'Eli', NULL, NULL),
  (6, NULL, NULL, 'C')
AS null_demo(id, name, amount, product);
-- Find rows where product is NULL
SELECT * FROM null_demo
WHERE product <=> NULL;
```

## Operator Behavior

operator|condition|result
---|---|---
=| NULL = 'abc'| → NULL (excluded)
<=> |NULL <=> NULL| → TRUE

## 🔄 Combine NULL filters with other logic

```sql
Copy
Edit
-- Filter non-null products starting with 'A'
SELECT * FROM products
WHERE product IS NOT NULL AND product LIKE 'A%';
```
