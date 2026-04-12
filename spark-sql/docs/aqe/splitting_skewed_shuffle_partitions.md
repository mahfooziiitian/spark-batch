# :material-auto-fix: Splitting Skewed Shuffle Partitions

AQE can split skewed shuffle partitions to avoid single tasks that process
much more data than others.

---

## 📌 How It Works

1. Detects partitions larger than `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`.
2. Splits large partitions into smaller chunks.
3. Processes chunks in parallel across tasks.

---

## 🧪 Example Configuration

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256000000;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Long tail tasks | Enable skew partition splitting |
| Highly skewed keys | Combine with key salting |
