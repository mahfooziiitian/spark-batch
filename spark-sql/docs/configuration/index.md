# :material-cog: Spark SQL Configuration

Spark SQL configuration settings control execution behaviour, performance, memory,
and feature flags. Settings can be applied at session scope (SQL `SET`) or cluster
scope (cluster config / `SparkConf`).

---

## :material-sitemap: In This Section

| Page | Covers |
|------|--------|
| [Shuffle & Partitioning](shuffle.md) | `shuffle.partitions`, `partitionOverwriteMode`, repartition hints |
| [Adaptive Query Execution](aqe.md) | AQE settings — skew join, coalesce, dynamic partition pruning |
| [Join Configuration](join.md) | Broadcast threshold, join strategy hints, sort-merge config |
| [Memory & Spill](memory.md) | Executor memory, spill settings, off-heap |
| [File & I/O Settings](io.md) | Parquet/ORC/Delta read-write settings, compression, file size |
| [Session Management](session.md) | `SET`, `RESET`, `SET -v`, scoping rules |

---

## :material-code-tags: Quick Reference

```sql
-- View all current settings
SET -v;

-- View a single setting
SET spark.sql.shuffle.partitions;

-- Set for the current session
SET spark.sql.shuffle.partitions = 50;

-- Reset a setting to its default
RESET spark.sql.shuffle.partitions;
```

---

## :material-information-outline: Scoping Rules

1. **Cluster-level** config (set in `spark-defaults.conf` or cluster UI) applies to all sessions.
2. **Session-level** `SET` overrides cluster config for the current SparkSession only — reset when the session ends.
3. `RESET key` reverts a session-level override back to the cluster/default value.
4. `RESET` (no argument) resets **all** session-level overrides.
5. Some settings (e.g., `spark.executor.memory`) are **static** — they cannot be changed after the SparkContext starts.
