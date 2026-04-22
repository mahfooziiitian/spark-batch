# :material-lightning-bolt: Caching

In Spark SQL, CACHE is used to persist the results of a table or query in memory across multiple operations to improve performance — especially if the same data is accessed repeatedly.

### :material-sitemap: Overview

```mermaid
graph LR
    A[First Query] --> B[CACHE TABLE]
    B --> C[In-Memory Store]
    C --> D[Subsequent Queries]
    D -->|Cache hit| C
    D -->|Cache miss| E[Recompute]
```

1. SQL CACHE TABLE is eager

## Syntax

The following gives "In-memory table `hundred`"

```sql
CACHE TABLE hundred
```

```sql
CACHE TABLE students;
SELECT * FROM students WHERE age > 20;
```

## :material-check-circle-outline: 2. Cache a Temporary View

```sql
CACHE TABLE my_temp_view;
```

You can also create and cache it inline:

```sql
CREATE OR REPLACE TEMP VIEW temp_students AS
SELECT * FROM students WHERE age > 20;

CACHE TABLE temp_students;
```

### :material-check-circle-outline: 4. Check What Is Cached

```sql
SHOW TABLES;
-- or specifically:
SHOW TABLE EXTENDED LIKE 'students';
```

### :material-check-circle-outline: 5. Uncache a Table

```sql
UNCACHE TABLE students;
```

### :material-check-circle-outline: 6. Remove All Caches

```sql
CLEAR CACHE;
```

## Configuration of in-memory caching

### spark.sql.inMemoryColumnarStorage.compressed

When set to true, Spark SQL will automatically select a compression codec for each column based on statistics of the data.

### spark.sql.inMemoryColumnarStorage.batchSize

Controls the size of batches for columnar caching.

Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data.

## When to Use Caching

When to Use                              | Benefit
-----------------------------------------|-------------------------------------
Query uses same table/view repeatedly    | Saves time by avoiding recomputation
Table fits in memory                     | Fastest access (memory vs. disk)
Intermediate query reused multiple times | Great performance improvement

## :material-alert:️ Notes

1. Lazy Evaluation: Cache is lazy. The first action (count, collect, show, etc.) triggers caching.
2. Memory Sensitive: If the data doesn’t fit in memory, Spark may spill to disk or evict older cached data.
3. CACHE TABLE persists in memory for the SparkSession only.
