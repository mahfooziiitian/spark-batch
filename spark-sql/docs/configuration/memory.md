# :material-memory: Memory & Spill Config

Memory and spill settings control how Spark allocates heap memory between execution
(shuffle, sort, aggregation) and storage (caching), and when it spills to disk.

---

## :material-code-tags: Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.executor.memory` | `1g` | Total JVM heap per executor |
| `spark.memory.fraction` | `0.6` | Fraction of heap for Spark execution + storage |
| `spark.memory.storageFraction` | `0.5` | Fraction of `memory.fraction` reserved for caching |
| `spark.sql.files.maxPartitionBytes` | `128MB` | Max bytes per input partition when reading files |
| `spark.sql.files.openCostInBytes` | `4MB` | Estimated cost to open a file (affects partition sizing) |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Tables below this are broadcast (memory trade-off) |
| `spark.executor.memoryOverhead` | `10%` of executor memory | Off-heap memory for JVM overhead, native code |

---

## :material-information-outline: Behavior

1. Spark's **unified memory model** shares a single pool between execution (sort/hash buffers) and storage (cached RDDs/DataFrames). Each can borrow from the other.
2. When execution memory is exhausted, data is **spilled to disk** — this is safe but slow. Spill appears as large `Shuffle Write` sizes in the Spark UI stage details.
3. `spark.memory.fraction = 0.6` means 60 % of the heap is available for Spark; the remaining 40 % is reserved for user code, metadata, and JVM overhead.
4. `spark.sql.files.maxPartitionBytes` controls input partition granularity — smaller values create more (smaller) tasks; larger values create fewer (larger) tasks.
5. `memoryOverhead` covers off-heap use by the JVM itself (thread stacks, direct buffers) — increase it for workloads using Python UDFs or Arrow-based operations.

---

## :material-flask-outline: Practical Examples

### Reduce input partition size for better parallelism

```sql
-- Default 128 MB — reduce to 64 MB for many small files
SET spark.sql.files.maxPartitionBytes = 67108864;  -- 64 MB

SELECT region, COUNT(*) FROM events GROUP BY region;

RESET spark.sql.files.maxPartitionBytes;
```

### Increase input partition size to reduce task count

```sql
-- Reduce overhead when processing large files with few cores
SET spark.sql.files.maxPartitionBytes = 268435456;  -- 256 MB

SELECT * FROM large_parquet_table WHERE event_date = '2024-06-01';

RESET spark.sql.files.maxPartitionBytes;
```

### Diagnose spill in a GROUP BY

```sql
-- If this query spills, reduce shuffle partitions so each partition is larger
-- and fits in the hash aggregate buffer
SET spark.sql.shuffle.partitions = 400;

SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id;
-- Check Spark UI: if Shuffle Spill (Disk) > 0, further reduce partitions or increase memory
```

### Control memory for broadcast

```sql
-- If broadcast tables cause executor OOM, lower the threshold
SET spark.sql.autoBroadcastJoinThreshold = 52428800;  -- 50 MB (down from 200 MB)
```

### Verify memory settings for the session

```sql
SET spark.executor.memory;
SET spark.memory.fraction;
SET spark.memory.storageFraction;
SET spark.sql.files.maxPartitionBytes;
```

---

## :material-lightbulb-outline: When to Tune Memory Settings

| Symptom | Setting to change |
|---------|-----------------|
| Tasks spilling to disk | Reduce `shuffle.partitions` or increase `executor.memory` |
| Executor OOM on broadcast | Lower `autoBroadcastJoinThreshold` |
| Too many small input tasks | Increase `maxPartitionBytes` |
| Python UDF / Arrow OOM | Increase `memoryOverhead` |
| Cached tables evicted too quickly | Increase `memory.storageFraction` |

!!! warning "Static settings"
    `spark.executor.memory` and `spark.memory.fraction` are **static** — they must be
    set in the cluster config or `SparkConf` before the SparkContext starts.
    SQL `SET` commands cannot change them at session time.
