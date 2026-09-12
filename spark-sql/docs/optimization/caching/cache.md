# :material-table-refresh: Cache Commands

This page focuses on the SQL surface area of caching: how to register cached data, how eager and lazy modes differ, and what `UNCACHE`, `CLEAR CACHE`, and `REFRESH TABLE` actually did in Spark 4.2.

______________________________________________________________________

## :material-animation-play: Interactive Visualization

### :material-animation-play: Interactive Visualization — Cache Lifecycle Commands

<div id="viz-cache-lifecycle" class="ts-viz"></div>

Use the controls to step through the normal cache lifecycle: registration, first materialization, reuse, explicit uncache, and a metadata refresh that does not itself drop the cache entry.

______________________________________________________________________

## :material-code-braces: Verified Syntax

### Eager cache

```sql
CACHE TABLE orders;
```

For an existing table or view, `CACHE TABLE` started a job immediately in Spark 4.2.

### Lazy cache

```sql
CACHE LAZY TABLE orders;
```

`CACHE LAZY TABLE` registered the cache entry immediately, but no fill happened until the first query. In PySpark 4.2, `spark.catalog.isCached('orders')` returned `True` right after registration.

### Cache a query result as a named relation

```sql
CACHE TABLE active_customers AS
SELECT customer_id, name, region
FROM customers
WHERE status = 'active';
```

### Pick a storage level directly in SQL

```sql
CACHE TABLE active_customers_mem
OPTIONS ('storageLevel' = 'MEMORY_ONLY') AS
SELECT customer_id, name, region
FROM customers
WHERE status = 'active';
```

Spark 4.2 accepted `OPTIONS ('storageLevel' = '...')` and showed the chosen level inside `EXPLAIN` output.

### Remove cache entries

```sql
UNCACHE TABLE orders;
UNCACHE TABLE IF EXISTS orders;
CLEAR CACHE;
```

`UNCACHE TABLE IF EXISTS` is valid Spark 4.2 syntax.

### Refresh metadata

```sql
REFRESH TABLE orders;
```

`REFRESH TABLE` is valid syntax, but it is **not** a synonym for `UNCACHE TABLE`.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### Eager cache shows an in-memory scan

```sql
CACHE TABLE t_cache AS
SELECT * FROM VALUES (1), (2) AS t(x);

EXPLAIN SELECT * FROM t_cache;
```

```text
== Physical Plan ==
Scan In-memory table t_cache [x#...]
   +- InMemoryRelation [x#...], StorageLevel(disk, memory, deserialized, 1 replicas)
         +- LocalTableScan [...]
```

### Lazy cache registers first, fills later

```sql
CACHE LAZY TABLE t_lazy AS
SELECT * FROM VALUES (10), (20) AS t(x);

SELECT * FROM t_lazy;
```

Before the first query, the cache entry existed but had not yet been read through the in-memory scan path. The first `SELECT` performed the initial fill.

### Querying after `UNCACHE TABLE`

```sql
UNCACHE TABLE t_cache;
SELECT * FROM t_cache;
```

The second statement still worked in Spark 4.2. `UNCACHE TABLE` removed cached data; it did not drop the relation name created by `CACHE TABLE ... AS SELECT ...`.

______________________________________________________________________

## :material-refresh: Refresh and Invalidation Notes

### What `REFRESH TABLE` did in local Spark 4.2 checks

For an unchanged cached temp view, `REFRESH TABLE view_name` left `spark.catalog.isCached(view_name)` as `True`, and `EXPLAIN SELECT ...` still showed `InMemoryRelation` / `Scan In-memory table`.

That means this page should avoid claiming that `REFRESH TABLE` automatically drops cached data. If your goal is to force the next query to recompute from the original source, use `UNCACHE TABLE`.

### Replacing a temp view is different

In a separate check, this sequence removed the cache registration:

```sql
CREATE OR REPLACE TEMP VIEW v_refresh AS SELECT * FROM VALUES (1), (2) AS t(x);
CACHE TABLE v_refresh;
CREATE OR REPLACE TEMP VIEW v_refresh AS SELECT * FROM VALUES (3), (4) AS t(x);
```

After `CREATE OR REPLACE TEMP VIEW` with the same name, `spark.catalog.isCached('v_refresh')` became `False`.

______________________________________________________________________

## :material-information-outline: Observability

There is no built-in SQL function such as `IS_CACHED(table_name)`.

What we verified instead:

- `SHOW TABLES` only showed whether the relation was temporary.
- `DESCRIBE EXTENDED` did not expose cached status for the tested temp view.
- `EXPLAIN SELECT ...` was the most direct SQL-side signal because cache hits showed `Scan In-memory table` and `InMemoryRelation`.
- Programmatically, `spark.catalog.isCached(name)` is the simplest check.

______________________________________________________________________

## :material-compare: `CACHE TABLE` vs `CACHE LAZY TABLE`

| Aspect                              |  `CACHE TABLE`  |         `CACHE LAZY TABLE`          |
| ----------------------------------- | :-------------: | :---------------------------------: |
| Registers a cache entry immediately |       Yes       |                 Yes                 |
| Triggers a fill job immediately     |       Yes       |                 No                  |
| Leaves the next query warm          |       Yes       |   Not until first access finishes   |
| Best fit                            | Known hot paths | Session startup or exploratory work |

______________________________________________________________________

## :material-play-circle-outline: Run

```bash
cd /home/malam/development/processing/batch/spark-batch/spark-sql && python3 - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("cache-commands-doc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CACHE LAZY TABLE demo_lazy AS SELECT * FROM VALUES (10), (20) AS t(x)")
print("cached right away:", spark.catalog.isCached("demo_lazy"))
spark.sql("SELECT * FROM demo_lazy").show()
spark.sql("UNCACHE TABLE IF EXISTS demo_lazy")

spark.stop()
PY
```
