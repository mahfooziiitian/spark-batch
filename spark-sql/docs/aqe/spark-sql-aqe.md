# Spark SQL AQE Overview

Adaptive Query Execution (AQE) re-optimizes query plans at runtime based on
actual data statistics.

---

## 📌 Benefits

| Benefit | Description |
|---------|-------------|
| Dynamic join selection | Switch to broadcast if smaller |
| Partition coalescing | Reduce tiny shuffle partitions |
| Skew handling | Split large partitions |

---

## 🧪 Enable AQE

```sql
SET spark.sql.adaptive.enabled = true;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Variable data sizes | Enable AQE |
| Skewed joins | AQE skew handling |
| Too many small partitions | Coalescing |
