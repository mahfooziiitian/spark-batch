# Lateral View Filtering

`LATERAL VIEW` expands arrays or maps into multiple rows using `explode`.
This makes it easy to filter based on individual array elements or map entries.

---

## 📌 Syntax

```sql
SELECT cols...
FROM table
LATERAL VIEW explode(array_col) AS element
WHERE element = 'value';
```

---

## 🧪 Practical Examples

### Explode an Array and Filter Elements

```sql
SELECT user_id, tag
FROM events
LATERAL VIEW explode(tags) AS tag
WHERE tag = 'gift';
```

### Explode a Map and Filter by Key/Value

```sql
SELECT order_id, k, v
FROM orders
LATERAL VIEW explode(attributes) AS k, v
WHERE k = 'priority' AND v = 'high';
```

### Preserve Rows with NULL Arrays

```sql
SELECT user_id, tag
FROM events
LATERAL VIEW explode_outer(tags) AS tag
WHERE tag IS NULL OR tag = 'promo';
```

---

## 🔍 Behavior Notes

1. `explode` drops rows where the array or map is NULL.
2. `explode_outer` preserves NULLs by emitting one row with NULL values.
3. Lateral views can increase row counts significantly; filter early when possible.

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Filter by individual array items | `LATERAL VIEW explode` |
| Filter map entries | `LATERAL VIEW explode(map)` |
| Keep NULLs | `explode_outer` |
| Avoid row explosion | Use `array_contains` when possible |

---

> **Tip:** For small arrays, `array_contains` is cheaper than exploding rows.
