# Deduplication

To de-duplicate data in Spark SQL, you typically want to remove rows that are exact duplicates or duplicates based on a specific key or set of columns.

## ✅ 1. Remove Exact Duplicates (All Columns Match)

If you want to remove completely duplicate rows.

```sql
SELECT DISTINCT *
FROM my_table
```

## ✅ 2. Remove Duplicates Based on Key Columns (Keep First)

If duplicates are based on certain key columns (e.g. id), and you want to keep the first row (by ingestion order or any arbitrary row).

```sql

SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY some_column) AS rn
  FROM my_table
)
WHERE rn = 1
```

## ✅ 3. Remove Duplicates Based on Key Columns (Keep Any Row)

If you just want any one row per key, without specifying an order.

```sql
SELECT id, FIRST(name) AS name
FROM my_table
GROUP BY id
```

## ✅ 4. Remove Duplicates and Keep the Latest Row (by timestamp)

```sql

SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_col DESC) AS rn
  FROM my_table
)
WHERE rn = 1
```
