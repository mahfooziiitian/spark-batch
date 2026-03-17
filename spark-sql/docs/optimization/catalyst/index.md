# Catalyst Optimizer

Catalyst is Spark SQL's rule-based and cost-based optimizer. It transforms
logical plans into efficient physical plans.

---

## 📌 Stages

| Stage | Description |
|-------|-------------|
| Analysis | Resolve columns and types |
| Optimization | Apply rewrite rules |
| Planning | Choose physical operators |

---

## 🧪 Example

```sql
EXPLAIN FORMATTED SELECT * FROM orders WHERE amount > 100;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Performance tuning | Inspect plans |
| Debugging queries | Use `EXPLAIN` |
