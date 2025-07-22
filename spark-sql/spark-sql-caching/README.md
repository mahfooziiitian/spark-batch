# caching table

In Spark SQL, caching is a way to persist query results or tables in memory (and optionally on disk) to speed up subsequent queries that reuse the same data. Caching avoids recomputing or rereading data from slower sources like disk or remote storage.

## 🔥 Spark SQL Caching Overview

1. Cache: Store data in memory for faster access.
2. Persist: Cache with different storage levels (memory-only, memory+disk, etc.).
3. Spark SQL lets you cache tables, views, or query results.
4. Useful when the same data is accessed multiple times in your session or notebook.

## How to Cache in Spark SQL

### 1. Cache a table or temporary view

```sql
CACHE TABLE tableName;
```

Example:

```sql
CACHE TABLE employees;
```

This caches the table in memory.

## 2. Cache a query result (SQL or DataFrame)

You can create a cached temporary view from a query:

```sql
CREATE OR REPLACE TEMP VIEW cached_view AS
SELECT * FROM employees WHERE salary > 100000;
CACHE TABLE cached_view;
```

## 3. Uncache a table or view

```sql
UNCACHE TABLE tableName;
```

Example:

```sql
UNCACHE TABLE employees;
```

## lazy caching

```sql

sql("CACHE LAZY TABLE [tableName]")
```

## When to Cache?

1. When the same table/view is queried multiple times.
2. When iterative algorithms or dashboards query the same data repeatedly.
3. To speed up joins or filters on cached data.
