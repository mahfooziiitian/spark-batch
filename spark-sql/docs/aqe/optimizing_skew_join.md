# AQE Skew Join Optimization

AQE can detect and split skewed shuffle partitions to reduce straggler tasks.

---

## 📌 Key Settings

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
```

---

## 🔍 Behavior Notes

1. Skew detection uses partition size thresholds.
2. Skewed partitions are split into smaller tasks.
3. Works best with sort-merge joins.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Skewed keys | Enable AQE skew join |
| Long tail stages | AQE partition splitting |
