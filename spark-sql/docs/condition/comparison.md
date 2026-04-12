# :material-compare: Comparison Conditions

Comparison predicates test equality or ordering between values.

---

## 📌 Operators

| Operator | Example | Notes |
|----------|---------|-------|
| `=` | `a = b` | Equality |
| `!=` or `<>` | `a != b` | Inequality |
| `>` `>=` `<` `<=` | `a >= 10` | Ordering |
| `<=>` | `a <=> b` | NULL-safe equality |
| `BETWEEN` | `x BETWEEN 1 AND 5` | Inclusive range |
| `IN` | `id IN (1,2,3)` | Membership |

---

## 🔍 Behavior Notes

1. **`BETWEEN` is inclusive** — `x BETWEEN 1 AND 5` includes 1 and 5.
2. **`IN` with NULLs** — If the list (or subquery) contains NULL, `NOT IN` can
   return no rows. Prefer `NOT EXISTS` for anti-joins.
3. **`<=>` for NULLs** — `NULL <=> NULL` is TRUE; `NULL = NULL` is NULL.

---

## 🧪 Practical Examples

### Range Filter

```sql
SELECT * FROM sessions
WHERE duration BETWEEN 60 AND 300;
```

### Membership Filter

```sql
SELECT * FROM users
WHERE country IN ('US', 'CA', 'UK');
```

### NULL-Safe Join Condition

```sql
SELECT *
FROM a
JOIN b
ON a.id <=> b.id;
```

---

## 🧠 When to Use

| Scenario | Pattern |
|----------|---------|
| Equality matches | `=` or `<=>` (if NULLs possible) |
| Range checks | `BETWEEN` |
| Lookup lists | `IN (...)` |
| Safe NULL comparisons | `<=>` |

---

> **Tip:** Use explicit `CAST()` when comparing strings to numbers to avoid
> unexpected type coercion.
