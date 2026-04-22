# :material-table-column: Column Expressions

Columns are the building blocks of SQL queries. You can select, alias, and
derive new columns using expressions and functions.

---

## :material-pin: Common Patterns

```sql
SELECT id, amount AS total_amount
FROM orders;
```

```sql
SELECT id, amount * 1.1 AS amount_with_tax
FROM orders;
```

---

## :material-magnify: Tips

1. Use aliases (`AS`) for readability.
2. Expressions can reference multiple columns.
3. Use built-in functions for date, string, and math operations.

---

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Rename columns | `col AS alias` |
| Derived metrics | `col1 + col2` |
| Cleaner SQL | Use aliases consistently |
