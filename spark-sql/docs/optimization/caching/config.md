# :material-wrench: Caching Configuration

This page covers the knobs that materially affected Spark SQL caching in local Spark 4.2 checks: in-memory columnar storage settings, storage-level selection, and join planning around cached data.

______________________________________________________________________

## :material-animation-play: Interactive Visualization

### :material-animation-play: Interactive Visualization — Storage-Level Tradeoffs

<div id="viz-cache-config-tradeoffs" class="ts-viz"></div>

The visualization compares common storage levels and shows how compression, disk fallback, and broadcast-threshold decisions interact with cached data.

______________________________________________________________________

## :material-table: Verified Spark 4.2 Settings

| Property                                       | Verified Spark 4.2 value                                               | What it controls                                                            |
| ---------------------------------------------- | ---------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `spark.sql.inMemoryColumnarStorage.compressed` | `true`                                                                 | Whether Spark compresses cached columnar batches                            |
| `spark.sql.inMemoryColumnarStorage.batchSize`  | `10000`                                                                | Rows per cached columnar batch                                              |
| `spark.sql.autoBroadcastJoinThreshold`         | `10485760b`                                                            | Broadcast cutoff used by join planning, including joins against cached data |
| `spark.sql.cache.serializer`                   | `org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer` | Serializer used for cached columnar batches                                 |

Two common corrections from this verification pass:

- `spark.sql.cache.level` was **not** present in Spark 4.2; `spark.conf.get('spark.sql.cache.level')` raised `SQL_CONF_NOT_FOUND`.
- Storage level for SQL caching is better documented through `CACHE TABLE ... OPTIONS ('storageLevel' = '...')` than through a session config that does not exist here.

______________________________________________________________________

## :material-database: Storage Levels

### Verified default

For both `CACHE TABLE ... AS SELECT ...` and `DataFrame.cache()`, Spark 4.2 reported this default storage level:

```text
StorageLevel(disk, memory, deserialized, 1 replicas)
```

That corresponds to the familiar memory-plus-disk, deserialized cache behavior.

### Verified SQL override examples

```sql
CACHE TABLE level_mem
OPTIONS ('storageLevel' = 'MEMORY_ONLY') AS
SELECT * FROM source_table;

CACHE TABLE level_disk
OPTIONS ('storageLevel' = 'DISK_ONLY') AS
SELECT * FROM source_table;
```

`EXPLAIN SELECT * FROM level_mem` showed `StorageLevel(memory, deserialized, 1 replicas)`, and the `DISK_ONLY` variant showed `StorageLevel(disk, 1 replicas)`.

### Practical comparison

| Level             | Memory | Disk fallback |   CPU profile   | Good fit                                                        |
| ----------------- | :----: | :-----------: | :-------------: | --------------------------------------------------------------- |
| `MEMORY_ONLY`     |  High  |      No       | Low decode cost | Small, repeatedly scanned hot data                              |
| `MEMORY_AND_DISK` | Medium |      Yes      | Low decode cost | Default choice when size is uncertain                           |
| `DISK_ONLY`       |  Low   |      Yes      | Higher IO cost  | Very large results where recomputation is worse than disk reads |

______________________________________________________________________

## :material-code-braces: Applying the Knobs

```sql
SET spark.sql.inMemoryColumnarStorage.compressed = true;
SET spark.sql.inMemoryColumnarStorage.batchSize = 20000;

CACHE TABLE orders_mem
OPTIONS ('storageLevel' = 'MEMORY_ONLY') AS
SELECT * FROM orders;
```

Use `SET` for the in-memory columnar settings and `OPTIONS ('storageLevel' = '...')` on the `CACHE TABLE` statement for the storage level itself.

______________________________________________________________________

## :material-call-split: Cached Tables and Broadcast Joins

Caching does **not** freeze join strategy. Spark still compares estimated size against `spark.sql.autoBroadcastJoinThreshold`.

In Spark 4.2 checks against a cached two-row dimension table:

- with `spark.sql.autoBroadcastJoinThreshold = -1`, Spark used `SortMergeJoin`;
- with `spark.sql.autoBroadcastJoinThreshold = 1`, Spark still used `SortMergeJoin`; and
- with the default `10485760b`, Spark switched to `BroadcastHashJoin` while still scanning the cached side through `Scan In-memory table`.

So the right mental model is: caching changes **where** rows come from, while the join threshold still influences **how** Spark joins them.

______________________________________________________________________

## :material-tune: Tuning Guide

| Symptom                                                   | Likely cause                                   | Prefer this response                                     |
| --------------------------------------------------------- | ---------------------------------------------- | -------------------------------------------------------- |
| Cache fill is expensive but later queries are few         | Over-caching                                   | Skip caching or cache a smaller derived result           |
| Cache reads are fast but entries disappear under pressure | Cache footprint too large                      | Keep fewer hot relations or use a smaller derived result |
| Cached dimension still is not broadcast                   | Broadcast threshold too low or stats too large | Revisit `spark.sql.autoBroadcastJoinThreshold`           |
| Executors struggle during fill                            | Batch size or footprint is too aggressive      | Lower `batchSize` or choose a different storage level    |

______________________________________________________________________

## :material-play-circle-outline: Run

```bash
cd /home/malam/development/processing/batch/spark-batch/spark-sql && python3 - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("cache-config-doc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("compressed =", spark.conf.get("spark.sql.inMemoryColumnarStorage.compressed"))
print("batchSize =", spark.conf.get("spark.sql.inMemoryColumnarStorage.batchSize"))
print("broadcastThreshold =", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

spark.sql("CACHE TABLE demo_mem OPTIONS('storageLevel'='MEMORY_ONLY') AS SELECT * FROM VALUES (1), (2) AS t(x)")
for row in spark.sql("EXPLAIN SELECT * FROM demo_mem").collect():
    print(row[0])

spark.stop()
PY
```
