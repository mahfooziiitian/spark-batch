# Deduplication

To de-duplicate data in Spark SQL, you typically want to remove rows that are exact duplicates or duplicates based on a specific key or set of columns.

## Remove duplicate with all column

### 1. Using DISTINCT

If you want to remove completely duplicate rows.

```sql
SELECT DISTINCT *
FROM my_table
```

### ✅ 2. Using ROW_NUMBER

If duplicates are based on certain key columns (e.g. id), and you want to keep the first row (by ingestion order or any arbitrary row).

```sql

SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY some_column) AS rn
  FROM my_table
)
WHERE rn = 1
```

### ✅ 2. Using RANK() or DENSE_RANK()

Using RANK() or DENSE_RANK() (For more complex tie-breaks).

```sql
WITH ranked AS (
  SELECT *,
         RANK() OVER (PARTITION BY name ORDER BY id ASC) AS rk
  FROM your_table
)
SELECT * FROM ranked
WHERE rk = 1;
```

### 3. Using JOIN on Deduplicated Keys

```sql
WITH dedup_keys AS (
  SELECT name, age, MIN(id) AS id
  FROM your_table
  GROUP BY name, age
)
SELECT t.*
FROM your_table t
JOIN dedup_keys d
ON t.id = d.id;
```

## ✅ 3. Remove Duplicates Based on Key Columns (Keep Any Row)

If you just want any one row per key, without specifying an order.

```sql
SELECT *
FROM (
  SELECT *, FIRST() OVER (PARTITION BY key_column ORDER BY some_column) AS rn
  FROM my_table
)
WHERE rn = 1
```

### Using WINDOW + DELETE on Delta Tables

```SQL
DELETE FROM your_table
WHERE id NOT IN (
  SELECT id FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY name, age ORDER BY id) AS rn
    FROM your_table
  ) WHERE rn = 1
);
```
