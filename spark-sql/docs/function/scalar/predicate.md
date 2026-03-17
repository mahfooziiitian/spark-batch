# Predicate Functions

Predicate functions return TRUE or FALSE and are often used in filters.

---

## 📌 Common Functions

| Function | Purpose |
|----------|---------|
| `ISNULL` | Check NULL |
| `ISNOTNULL` | Check non-NULL |
| `ISNAN` | Check NaN |
| `INSTR` | Substring check |

---

## 🧪 Example

```sql
SELECT * FROM metrics WHERE ISNAN(value) = false;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Null checks | `ISNULL` / `ISNOTNULL` |
| NaN detection | `ISNAN` |
