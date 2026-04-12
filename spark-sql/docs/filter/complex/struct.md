# :material-code-braces: Struct Filtering

Structs store nested fields. You can filter by nested fields using dot notation.

---

## 📌 Accessing Nested Fields

```sql
SELECT * FROM users
WHERE address.city = 'New York';
```

You can also access by field name with `struct_col['field']` in some Spark
versions, but dot notation is the most common in Spark SQL.

---

## 🧪 Practical Examples

### Filter by Nested Field

```sql
SELECT * FROM customers
WHERE profile.tier = 'gold';
```

### Combine Nested Filters

```sql
SELECT * FROM customers
WHERE profile.tier = 'gold'
  AND profile.region = 'US';
```

### Handle NULL Structs

```sql
SELECT * FROM customers
WHERE profile IS NOT NULL
  AND profile.tier = 'gold';
```

---

## 🔍 Behavior Notes

1. If the struct itself is NULL, any nested field access returns NULL.
2. Use `struct_col IS NOT NULL` to guard nested field comparisons.
3. Nested field filters can still be pushed down if supported by the data source.

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Filter on nested attributes | `struct_col.field` |
| Avoid NULL errors | Add `struct_col IS NOT NULL` |
| Complex nesting | Use multiple dotted paths |

---

> **Tip:** For repeated nested filters, consider selecting nested fields into
> top-level columns in a view.
