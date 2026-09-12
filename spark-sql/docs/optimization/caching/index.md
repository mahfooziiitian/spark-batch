# :material-lightning-bolt: Caching Overview

Spark SQL caching keeps a reusable, session-scoped copy of a query result so later queries can skip part or all of the original computation.

______________________________________________________________________

## :material-animation-play: Interactive Visualization

### :material-animation-play: Interactive Visualization — Cache Match vs Recompute

<div id="viz-caching-overview" class="ts-viz"></div>

This visualization shows the main question Spark asks after a cache is registered: does the incoming query contain a logically equivalent sub-plan that can be replaced with cached data?

______________________________________________________________________

## :material-sitemap: What We Verified in Spark 4.2

The four caching pages still divide cleanly by scope:

- `index.md` explains when caching helps and what a cache hit looks like.
- `cache.md` covers SQL commands such as `CACHE TABLE`, `UNCACHE TABLE`, `CLEAR CACHE`, and `REFRESH TABLE`.
- `config.md` covers storage-level choices and relevant SQL runtime settings.
- `manager.md` covers the internal `CacheManager` rewrite step and plan matching.

In local PySpark 4.2 tests, a cached table produced a physical plan shaped like this:

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

That confirms two important behaviors:

1. a cache hit rewrites the query to read from `InMemoryRelation`, and
2. the physical operator is the in-memory scan path, not the original source scan.

______________________________________________________________________

## :material-compare: When Caching Helps

| Scenario                                                              |   Cache?    | Why                                                       |
| --------------------------------------------------------------------- | :---------: | --------------------------------------------------------- |
| Same table or derived result reused several times in one session      |     Yes     | Avoids repeated scans, projections, filters, and UDF work |
| Large base table read once in a pipeline                              |     No      | The fill cost adds work without a later payoff            |
| Small filtered or aggregated result reused by many downstream queries |     Yes     | Cache the reduced result, not always the raw source       |
| Data or view definition changes constantly                            | Selectively | Revisit cache lifecycle and refresh behavior              |
| Streaming source or continuously changing relation                    | Usually no  | Batch-style cache semantics are a poor fit                |

______________________________________________________________________

## :material-flask-outline: Verified Behavior Gotchas

### Logical-plan matching matters more than table names

Spark 4.2 reused a cached result for this query even though it did **not** reference the cached table name:

```sql
CACHE TABLE cached_range AS
SELECT id, id % 2 AS grp FROM RANGE(0, 10);

SELECT id, id % 2 AS grp FROM RANGE(0, 10);
```

The optimized plan still contained `InMemoryRelation`, which means Spark matched the cached logical plan by equivalence, not only by relation name.

### A broader query does not reuse a narrower cached result

The reverse did **not** happen:

```sql
CACHE TABLE odd_ids AS
SELECT * FROM RANGE(0, 10) WHERE id % 2 = 1;

SELECT * FROM RANGE(0, 10);      -- no cache hit
SELECT * FROM RANGE(0, 10) WHERE id = 1;  -- no cache hit
```

Caching a filtered subset does not automatically satisfy a broader or differently shaped query.

### `UNCACHE TABLE` drops the cache, not the table/view name

After `UNCACHE TABLE t_cache`, the relation was still queryable; Spark simply fell back to the uncached plan.

______________________________________________________________________

## :material-code-braces: Quick Reference

```sql
-- Eagerly materialize an existing table or view
CACHE TABLE orders;

-- Register now, fill on first use
CACHE LAZY TABLE orders;

-- Cache a named query result
CACHE TABLE clean_orders AS
SELECT order_id, LOWER(TRIM(region)) AS region, amount
FROM raw_orders
WHERE order_id IS NOT NULL;

-- Drop one cache entry
UNCACHE TABLE orders;
UNCACHE TABLE IF EXISTS orders;

-- Drop every cache entry in the current SparkSession
CLEAR CACHE;
```

______________________________________________________________________

## :material-book-open-variant: In This Section

| Page                        | Focus                                                                    |
| --------------------------- | ------------------------------------------------------------------------ |
| [Cache Commands](cache.md)  | Verified SQL syntax, eager vs lazy behavior, refresh and uncache gotchas |
| [Configuration](config.md)  | Real Spark 4.2 defaults, storage levels, and join-planning interactions  |
| [Cache Manager](manager.md) | `CacheManager`, `InMemoryRelation`, and plan-equivalence matching        |

______________________________________________________________________

## :material-play-circle-outline: Run

```bash
cd /home/malam/development/processing/batch/spark-batch/spark-sql && python3 - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("cache-overview-doc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CACHE TABLE demo_cache AS SELECT * FROM VALUES (1), (2) AS t(x)")
for row in spark.sql("EXPLAIN SELECT * FROM demo_cache").collect():
    print(row[0])

spark.stop()
PY
```
