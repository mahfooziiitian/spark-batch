# Repartition Hint

`REPARTITION` hints force a shuffle to increase or decrease the number of
partitions and improve distribution.

---

## 📌 Syntax

```sql
SELECT /*+ REPARTITION(n) */ * FROM table;
SELECT /*+ REPARTITION(col1) */ * FROM table;
```

---

## 🔍 Behavior Notes

1. Triggers a full shuffle.
2. Use for balancing skewed datasets.
3. Prefer `COALESCE` for reducing partitions without shuffle.

---

## 🧪 Example

```sql
SELECT /*+ REPARTITION(200) */ *
FROM large_table;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Severe skew | Repartition on key |
| Too few partitions | Increase partition count |
| Too many small files | Repartition before write |
