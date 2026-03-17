# Query Planner

The physical planner converts an optimized logical plan into an executable physical plan
by selecting concrete algorithms for each operation.

## 📌 Join Strategies

| Strategy | When Used | Broadcast? |
|----------|-----------|------------|
| **Broadcast Hash Join** | One side fits in memory (< `spark.sql.autoBroadcastJoinThreshold`) | Yes |
| **Sort Merge Join** | Default for large-large joins | No |
| **Shuffle Hash Join** | One side is much smaller but above broadcast threshold | No |
| **Broadcast Nested Loop Join** | Non-equi joins with small table | Yes |
| **Cartesian Product** | Cross joins | No |

## 📌 Aggregation Strategies

| Strategy | Description |
|----------|-------------|
| **Hash Aggregate** | In-memory hash table; fast but memory-bound |
| **Sort Aggregate** | Sort-based; handles large groups with spilling |
| **Object Hash Aggregate** | For complex types (structs, arrays) |

## 🧪 Hint-Based Control

```sql
-- Force broadcast join
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table JOIN small_table ON large_table.id = small_table.id;

-- Force sort merge join
SELECT /*+ MERGE(t1, t2) */ *
FROM t1 JOIN t2 ON t1.id = t2.id;

-- Force shuffle hash join
SELECT /*+ SHUFFLE_HASH(t1) */ *
FROM t1 JOIN t2 ON t1.id = t2.id;

-- Repartition hint
SELECT /*+ REPARTITION(10) */ * FROM large_table;
```

## 📌 Key Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Max size for auto-broadcast |
| `spark.sql.shuffle.partitions` | `200` | Number of shuffle partitions |
| `spark.sql.adaptive.enabled` | `true` (3.x) | Enable Adaptive Query Execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Auto-coalesce small partitions |

## 🧠 When to Tune

| Symptom | Possible Fix |
|---------|-------------|
| Slow joins on small tables | Increase broadcast threshold or add `BROADCAST` hint |
| Too many small output files | Reduce shuffle partitions or enable AQE coalescing |
| Skewed joins | Enable AQE skew join optimization |
| OOM on aggregation | Reduce hash aggregate threshold |
