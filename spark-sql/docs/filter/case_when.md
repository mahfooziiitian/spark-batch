# CASE WHEN Filters

`CASE WHEN` is a conditional expression that returns a value. It is often used
for **classification** and **flagging** and can be combined with `WHERE` to
filter based on derived logic.

---

## 📌 Syntax

```sql
CASE
  WHEN condition THEN result
  [WHEN condition THEN result ...]
  [ELSE default_result]
END
```

---

## 🔍 Behavior

1. **Returns a value** — `CASE` produces a scalar value that can be selected or
   compared in a filter.
2. **First match wins** — Conditions are evaluated top-down; the first match is
   returned.
3. **ELSE is optional** — If no `ELSE` is provided and no condition matches,
   `CASE` returns NULL.
4. **Use in WHERE** — A `CASE` expression can be used in `WHERE`, but it must
   evaluate to a boolean (or a value you compare to).

---

## 🧪 Practical Examples

### Create a Flag and Filter on It

```sql
WITH sales AS (
  SELECT * FROM VALUES
    ('Alice', 100),
    ('Bob', 250),
    ('Charlie', 400)
  AS sales(name, amount)
)
SELECT *
FROM (
  SELECT *,
         CASE
           WHEN amount >= 300 THEN 'high'
           WHEN amount >= 150 THEN 'medium'
           ELSE 'low'
         END AS amount_level
  FROM sales
)
WHERE amount_level IN ('high', 'medium');
```

### CASE in a WHERE Clause

```sql
SELECT *
FROM orders
WHERE CASE
        WHEN status IS NULL THEN false
        WHEN status = 'cancelled' THEN false
        ELSE true
      END;
```

### Conditional Filter with NULL-Safe Logic

```sql
SELECT *
FROM products
WHERE CASE
        WHEN price IS NULL THEN false
        WHEN price > 100 THEN true
        ELSE false
      END;
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Create labeled buckets | `CASE WHEN` in SELECT |
| Filter on derived buckets | Wrap in a subquery and `WHERE` | 
| Replace complex OR logic | `CASE` to create a boolean flag |
| Avoid NULL surprises | Add explicit `ELSE` clauses |

---

> **Tip:** Prefer `WHERE` with direct boolean expressions when possible. Use
> `CASE WHEN` when you need reusable labels or complex branching.
