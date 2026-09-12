# :material-memory: Cache Manager

`CacheManager` is the part of Spark SQL that records cached logical plans and rewrites later queries to use `InMemoryRelation` when it finds an equivalent sub-plan.

______________________________________________________________________

## :material-animation-play: Interactive Visualization

### :material-animation-play: Interactive Visualization — CacheManager Match Rules

<div id="viz-cache-manager-match" class="ts-viz"></div>

This visualization focuses on the internal lookup decision: which incoming query shapes matched a cached logical plan in Spark 4.2, and which ones missed.

______________________________________________________________________

## :material-toy-brick: Internal Components

| Component          | Class                                                                  | Role                                 |
| ------------------ | ---------------------------------------------------------------------- | ------------------------------------ |
| Cache registry     | `org.apache.spark.sql.CacheManager`                                    | Tracks cached logical plans          |
| Logical cache node | `org.apache.spark.sql.execution.columnar.InMemoryRelation`             | Replaces a matching logical sub-plan |
| Physical scan node | `org.apache.spark.sql.execution.columnar.InMemoryTableScanExec`        | Reads cached columnar batches        |
| Batch serializer   | `org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer` | Encodes cached columnar batches      |

We verified the physical operator class directly in PySpark 4.2:

```python
spark.sql("CACHE TABLE t1 AS SELECT * FROM VALUES (1), (2) AS t(x)")
qe = spark.sql("SELECT * FROM t1")._jdf.queryExecution()
qe.executedPlan().getClass().getName()

# org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
```

______________________________________________________________________

## :material-format-list-numbered: What Happens on a Cache Hit

1. `CACHE TABLE` registers a logical plan with `CacheManager`.
2. Spark stores the cached form as an `InMemoryRelation`.
3. A later query is optimized.
4. During optimization, Spark looks for logically equivalent sub-plans.
5. When it finds one, that sub-plan is replaced with `InMemoryRelation`.
6. Physical planning emits `InMemoryTableScanExec` instead of scanning the original source again.

______________________________________________________________________

## :material-compare: What Matched in Spark 4.2

Starting from this cached query:

```sql
CACHE TABLE cached_range AS
SELECT id, id % 2 AS grp FROM RANGE(0, 10);
```

These follow-up queries all reused the cache:

| Query shape                                                                  | Cache hit? | Why                                                           |
| ---------------------------------------------------------------------------- | :--------: | ------------------------------------------------------------- |
| `SELECT * FROM cached_range`                                                 |    Yes     | Same named relation                                           |
| `SELECT * FROM cached_range AS cr`                                           |    Yes     | Alias keeps the same underlying logical plan                  |
| `SELECT * FROM cached_range WHERE grp = 1`                                   |    Yes     | Spark can apply an extra filter on top of the cached superset |
| `SELECT id, id % 2 AS grp FROM RANGE(0, 10)`                                 |    Yes     | Same logical computation, even without the cached table name  |
| `SELECT * FROM (SELECT id, id % 2 AS grp FROM RANGE(0, 10)) q WHERE grp = 1` |    Yes     | Matching happened inside the subquery before the outer filter |

These did **not** reuse the cache:

| Query shape                                                                       | Cache hit? | Why                                                                    |
| --------------------------------------------------------------------------------- | :--------: | ---------------------------------------------------------------------- |
| `SELECT id, (id + 1) % 2 AS grp FROM RANGE(0, 10)`                                |     No     | Different logical expression                                           |
| Broader query against an uncached superset when only a filtered subset was cached |     No     | Cached subset cannot stand in for missing rows                         |
| `SELECT * FROM RANGE(0, 10) WHERE id = 1` when only `WHERE id % 2 = 1` was cached |     No     | The plans are not equivalent; one is not a direct rewrite of the other |

______________________________________________________________________

## :material-magnify: Verified Plan Fragments

A cache hit optimized to `InMemoryRelation`:

```text
Filter (isnotnull(grp#...) AND (grp#... = 1))
+- InMemoryRelation [id#..., grp#...], StorageLevel(disk, memory, deserialized, 1 replicas)
```

A miss kept the original logical plan:

```text
Project [id#..., ((id#... + 1) % 2) AS grp#...]
+- Range (0, 10, step=1)
```

Those fragments are the clearest evidence that this page should talk about plan equivalence, not only about relation names.

______________________________________________________________________

## :material-alert-outline: Scope Notes and Cautions

- `CacheManager` decides whether a cached logical result can replace part of a query.
- Memory residency and block eviction happen below that rewrite step; this page therefore avoids making stronger runtime claims than we directly verified here.
- If you need to prove a cache hit during debugging, inspect `EXPLAIN` output or the executed-plan class name rather than assuming that a reused table name implies caching.

______________________________________________________________________

## :material-play-circle-outline: Run

```bash
cd /home/malam/development/processing/batch/spark-batch/spark-sql && python3 - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("cache-manager-doc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CACHE TABLE cached_range AS SELECT id, id % 2 AS grp FROM RANGE(0, 10)")
qe = spark.sql("SELECT id, id % 2 AS grp FROM RANGE(0, 10)")._jdf.queryExecution()
print(qe.optimizedPlan().toString())
print(qe.executedPlan().getClass().getName())

spark.stop()
PY
```
