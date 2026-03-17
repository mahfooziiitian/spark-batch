# Rebalance

`REBALANCE` evenly redistributes data across partitions to reduce skew and
improve parallelism.

---

## 📌 Syntax

```sql
SELECT /*+ REBALANCE(n) */ * FROM table;
SELECT /*+ REBALANCE(col1, col2) */ * FROM table;
```

---

## 🔍 Behavior

1. Triggers a shuffle to distribute data evenly.
2. Can be used without a specific key for uniform distribution.
3. Often improves performance for downstream joins and aggregations.

---

## 🧪 Example

```sql
SELECT /*+ REBALANCE(200) */ *
FROM large_fact_table;
```

---

## 🧠 When to Use

| Scenario | Use |
|----------|-----|
| Severe skew | `REBALANCE` |
| Improve parallelism | `REBALANCE(n)` |
| Reduce shuffle skew | `REBALANCE(col)` |
