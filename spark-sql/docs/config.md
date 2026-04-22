# :material-cog: Spark SQL Configuration

Configuration settings control execution behavior, performance, and features.
You can set options at session or cluster scope.

---

## :material-pin: Common Commands

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.shuffle.partitions = 200;

SET -v; -- Show all settings
```

---

## :material-magnify: Popular Settings

| Setting | Purpose |
|---------|---------|
| `spark.sql.shuffle.partitions` | Number of shuffle partitions |
| `spark.sql.adaptive.enabled` | Enable AQE |
| `spark.sql.autoBroadcastJoinThreshold` | Broadcast join threshold |
| `spark.sql.sources.partitionOverwriteMode` | Static vs dynamic overwrite |

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Large joins | Increase shuffle partitions |
| Skewed data | Enable AQE |
| Many small files | Repartition before write |
