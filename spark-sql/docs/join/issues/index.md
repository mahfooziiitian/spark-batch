# Join Issues

Join problems often show up as slow performance or incorrect results.
This guide lists common pitfalls and quick fixes.

---

## 📌 Common Issues

| Issue | Symptom | Fix |
|-------|---------|-----|
| Duplicate columns | Ambiguous references | Use aliases |
| Skewed keys | Long tail tasks | Use AQE, salting |
| Data explosion | Output much larger | Check join condition |

---

## 🧠 Tips

1. Validate join keys and nullability.
2. Use `EXPLAIN` to inspect strategy.
3. Broadcast small tables to avoid shuffle.

---

### Related Guides

- [Duplicate Columns](column_duplicate.md)
- [Skew Join](../optimization/skewjoin/index.md)
