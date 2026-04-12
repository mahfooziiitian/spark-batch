# :material-code-array: Array Filtering

Arrays require specialized functions to check for membership or conditions.
Spark SQL provides `array_contains`, higher-order functions, and indexing
helpers for flexible array filtering.

---

## 📌 Common Functions

| Function | Purpose |
|----------|---------|
| `array_contains` | Check if a value exists in an array |
| `exists` | Test if any element matches a predicate |
| `filter` | Keep array elements matching a predicate |
| `size` | Count elements |
| `element_at` | Access element by position |

---

## 🧪 Practical Examples

### Check Membership

```sql
SELECT * FROM events
WHERE array_contains(tags, 'priority');
```

### Use `exists` for Complex Conditions

```sql
SELECT * FROM events
WHERE exists(tags, t -> t LIKE 'promo%');
```

### Filter Arrays and Then Test Size

```sql
SELECT * FROM events
WHERE size(filter(tags, t -> t = 'gift')) > 0;
```

### Filter by First Element

```sql
SELECT * FROM events
WHERE element_at(tags, 1) = 'gift';
```

---

## 🔍 Behavior Notes

1. Array indexes are **1-based** in Spark SQL.
2. `exists` and `filter` are higher-order functions; they can be slower on very
   large arrays but provide strong expressiveness.
3. `array_contains` is usually faster than `exists` for simple membership checks.

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Simple membership | `array_contains` |
| Regex or complex logic | `exists` |
| Keep only matching rows | `size(filter(...)) > 0` |
| Position-specific checks | `element_at` |

---

> **See also:** [Lateral View](lateral_view.md) for exploding arrays into rows.
