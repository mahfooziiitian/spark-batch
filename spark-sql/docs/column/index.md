# :material-table-column: Column Expressions

Columns are the fundamental unit of a SQL query. Every `SELECT` clause produces a
list of column expressions — raw column references, computed values, aliases, casts,
and derived metrics.

---

## :material-sitemap: In This Section

| Page | Covers |
|------|--------|
| [Aliases](alias.md) | `AS`, expression aliases, quoting rules, scope |
| [Casting](casting.md) | `CAST`, `TRY_CAST`, implicit coercion, type precedence |
| [Derived Columns](derived.md) | Computed columns, expressions, conditional derivation |
| [Column Selection](selection.md) | `SELECT *`, column exclusion, reordering, `EXCEPT` clause |
| [Struct & Nested Columns](nested.md) | Dot-notation access, `struct`, `map`, `array` column patterns |
| [Column Defaults](defaults.md) | `DEFAULT`, `GENERATED ALWAYS AS`, `GENERATED ALWAYS AS IDENTITY` |
| [Renaming & DDL](renaming.md) | `ALTER TABLE RENAME COLUMN`, `ALTER COLUMN`, `DROP COLUMN` |

---

## :material-code-tags: Quick Reference

```sql
-- Alias
SELECT amount * 1.1 AS amount_with_tax FROM orders;

-- Cast
SELECT CAST(price AS DECIMAL(18, 2)) AS price FROM products;

-- Conditional derived column
SELECT
    amount,
    CASE WHEN amount >= 1000 THEN 'Large' ELSE 'Small' END AS order_size
FROM orders;

-- Nested column access
SELECT address.city, address.country FROM customers;

-- Column with default
INSERT INTO events (event_id, event_type)
VALUES (1, 'click');        -- created_at uses DEFAULT current_timestamp()
```
