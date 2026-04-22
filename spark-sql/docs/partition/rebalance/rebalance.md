# :material-scale-balance: Rebalance

`REBALANCE` evenly redistributes data across partitions to reduce skew and
improve parallelism.

---

## :material-pin: Syntax

```sql
SELECT /*+ REBALANCE(n) */ * FROM table;
SELECT /*+ REBALANCE(col1, col2) */ * FROM table;
```

---

## :material-magnify: Behavior

1. Triggers a shuffle to distribute data evenly.
2. Can be used without a specific key for uniform distribution.
3. Often improves performance for downstream joins and aggregations.

---

## :material-flask-outline: Example

```sql
SELECT /*+ REBALANCE(200) */ *
FROM large_fact_table;
```

---

## :material-brain: When to Use

| Scenario | Use |
|----------|-----|
| Severe skew | `REBALANCE` |
| Improve parallelism | `REBALANCE(n)` |
| Reduce shuffle skew | `REBALANCE(col)` |
