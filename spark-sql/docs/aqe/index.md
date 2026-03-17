# Adaptive Query Execution (AQE)

AQE adjusts query plans at runtime based on actual data statistics.
It can optimize join strategies, shuffle partitions, and skew handling.

---

## 📌 Key Features

| Feature | Benefit |
|---------|---------|
| Join strategy changes | Switch to broadcast if smaller than expected |
| Shuffle coalescing | Reduce small partitions |
| Skew handling | Split skewed partitions |

---

## 🧪 Example

```sql
SET spark.sql.adaptive.enabled = true;
SELECT * FROM big_table JOIN small_dim USING (id);
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Variable data sizes | Enable AQE |
| Skewed joins | AQE skew handling |
| Too many small partitions | AQE coalescing |
