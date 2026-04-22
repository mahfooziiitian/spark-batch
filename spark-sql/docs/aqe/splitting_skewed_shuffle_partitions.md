# :material-auto-fix: Splitting Skewed Shuffle Partitions

AQE can split skewed shuffle partitions to avoid single tasks that process
much more data than others.

---

## :material-pin: How It Works

1. Detects partitions larger than `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`.
2. Splits large partitions into smaller chunks.
3. Processes chunks in parallel across tasks.

---

## :material-flask-outline: Example Configuration

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256000000;
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Long tail tasks | Enable skew partition splitting |
| Highly skewed keys | Combine with key salting |
