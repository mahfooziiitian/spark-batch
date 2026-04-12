# :material-map: Map Filtering

Maps can be filtered by key, by value, or by applying higher-order functions.

---

## 📌 Common Functions

| Function | Purpose |
|----------|---------|
| `element_at` | Access value by key |
| `map_keys` | Return keys array |
| `map_values` | Return values array |
| `map_filter` | Keep entries matching a predicate |
| `size` | Count entries |

---

## 🧪 Practical Examples

### Filter by Key Value

```sql
SELECT * FROM orders
WHERE element_at(attributes, 'status') = 'delayed';
```

### Filter by Existence of a Key

```sql
SELECT * FROM orders
WHERE array_contains(map_keys(attributes), 'priority');
```

### Filter by Value Condition

```sql
SELECT * FROM orders
WHERE CAST(element_at(attributes, 'amount') AS DOUBLE) > 100;
```

### Use `map_filter` for Advanced Logic

```sql
SELECT * FROM orders
WHERE size(map_filter(attributes, (k, v) -> k = 'promo' AND v = 'true')) > 0;
```

---

## 🔍 Behavior Notes

1. `element_at(map, key)` returns NULL when the key is missing.
2. `map_keys` and `map_values` return arrays that can be filtered further.
3. `map_filter` is expressive but can be slower than direct `element_at` checks.

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Exact key lookup | `element_at` |
| Check if key exists | `array_contains(map_keys(...), key)` |
| Filter by key + value | `map_filter` + `size` |
| Value comparisons | `CAST(element_at(...))` |

---

> **See also:** [Lateral View](lateral_view.md) for exploding maps into rows.
