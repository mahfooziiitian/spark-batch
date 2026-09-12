# :material-memory: Memory & Spill Config

Memory and spill settings control how Spark allocates heap memory between execution
(shuffle, sort, aggregation) and storage (caching), and when it spills to disk.

### :material-animation-play: Interactive Visualization — Unified Memory Pressure

Toggle between a balanced workload, a cache-heavy workload, and an execution spike to see
how Spark's unified memory pool shifts between storage and execution. The last view shows
when a working set outgrows the pool and disk spill becomes unavoidable.

<div id="viz-config-memory" class="ts-viz"></div>

<script src="../assets/js/configuration-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Key Settings

| Setting                                | Default                  | Description                                              |
| -------------------------------------- | ------------------------ | -------------------------------------------------------- |
| `spark.executor.memory`                | `1g`                     | Total JVM heap per executor                              |
| `spark.memory.fraction`                | `0.6`                    | Fraction of heap for Spark execution + storage           |
| `spark.memory.storageFraction`         | `0.5`                    | Fraction of `memory.fraction` reserved for caching       |
| `spark.sql.files.maxPartitionBytes`    | `128MB`                  | Max bytes per input partition when reading files         |
| `spark.sql.files.openCostInBytes`      | `4MB`                    | Estimated cost to open a file (affects partition sizing) |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB`                   | Tables below this are broadcast (memory trade-off)       |
| `spark.executor.memoryOverhead`        | `10%` of executor memory | Off-heap memory for JVM overhead, native code            |

______________________________________________________________________

## :material-information-outline: Behavior

1. Spark's **unified memory model** shares a single pool between execution (sort/hash buffers) and storage (cached data). Each side can borrow from the other.
2. Execution memory can reclaim borrowed space from storage, but once the working set exceeds the unified pool Spark starts **spilling to disk**. Spill is safe, not free.
3. `spark.memory.fraction = 0.6` means 60 % of the heap is available for Spark; the remaining 40 % is reserved for user code, metadata, and JVM needs.
4. `spark.sql.files.maxPartitionBytes` controls input partition granularity — smaller values create more tasks; larger values create fewer, heavier tasks.
5. `memoryOverhead` covers off-heap use by the JVM itself (thread stacks, direct buffers, Python worker memory). Increase it for Python UDF, Arrow, or JNI-heavy workloads.
6. The Spark UI exposes spill directly through metrics such as `Spill (Memory)` and `Spill (Disk)` on SQL and stage detail pages.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Reduce input partition size for better parallelism

```sql
SET spark.sql.files.maxPartitionBytes = 67108864;  -- 64 MB

SELECT region, COUNT(*) FROM events GROUP BY region;

RESET spark.sql.files.maxPartitionBytes;
```

### Increase input partition size to reduce task count

```sql
SET spark.sql.files.maxPartitionBytes = 268435456;  -- 256 MB

SELECT * FROM large_parquet_table WHERE event_date = '2024-06-01';

RESET spark.sql.files.maxPartitionBytes;
```

### Reduce spill in a large GROUP BY

```sql
-- More shuffle partitions means less data per task and lower per-task memory pressure
SET spark.sql.shuffle.partitions = 400;

SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id;
-- Check the Spark UI: if Spill (Disk) stays high, raise partitions further or add executor memory
```

### Clamp broadcast size when executors are memory-bound

```sql
-- Lower the build-side size Spark will try to broadcast automatically
SET spark.sql.autoBroadcastJoinThreshold = 5242880;  -- 5 MB
```

### Verify memory settings for the session

```sql
SET spark.executor.memory;
SET spark.memory.fraction;
SET spark.memory.storageFraction;
SET spark.sql.files.maxPartitionBytes;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Tune Memory Settings

| Symptom                           | Setting to change                                           |
| --------------------------------- | ----------------------------------------------------------- |
| Tasks spilling to disk            | Increase `shuffle.partitions` or increase `executor.memory` |
| Executor OOM on broadcast         | Lower `autoBroadcastJoinThreshold`                          |
| Too many small input tasks        | Increase `maxPartitionBytes`                                |
| Python UDF or Arrow OOM           | Increase `memoryOverhead`                                   |
| Cached tables evicted too quickly | Increase `memory.storageFraction`                           |

!!! warning "Static settings"

    `spark.executor.memory`, `spark.memory.fraction`, and `spark.executor.memoryOverhead`
    are **static**. Set them in cluster config or `SparkConf` before the SparkContext
    starts; SQL `SET` can inspect them but cannot re-size a running application.
