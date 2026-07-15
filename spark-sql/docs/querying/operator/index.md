# :material-math-integral: SQL Operators

Operators combine values, compare expressions, and control query logic. Spark SQL
supports arithmetic, comparison, logical, string, set, bitwise, and null-safe operators.

---

## :material-sitemap: In This Section

| Page | Covers |
|------|--------|
| [Arithmetic](arithmetic.md) | `+`, `-`, `*`, `/`, `%`, `DIV`, `MOD`, operator precedence |
| [Comparison](comparison.md) | `=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`, `<=>`, `BETWEEN`, `LIKE` |
| [Logical](logical.md) | `AND`, `OR`, `NOT`, short-circuit evaluation, truth tables |
| [String](string.md) | `\|\|` concat, `LIKE`, `ILIKE`, `RLIKE`, `SIMILAR TO` |
| [Set](set.md) | `IN`, `NOT IN`, `BETWEEN`, `UNION`, `INTERSECT`, `EXCEPT` |
| [Null-Safe](null_safe.md) | `<=>`, `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM` |
| [Bitwise](bitwise.md) | `&`, `\|`, `^`, `~`, `<<`, `>>` |

---

## :material-code-tags: Quick Reference

```sql
-- Arithmetic
SELECT 10 + 5, 10 - 3, 10 * 2, 10 / 4, 10 % 3, 10 DIV 3;

-- Comparison
SELECT * FROM orders WHERE amount >= 100 AND status != 'CANCELLED';

-- Null-safe equality
SELECT * FROM orders WHERE region <=> NULL;   -- true when both are NULL

-- String concat
SELECT first_name || ' ' || last_name AS full_name FROM employees;

-- Set membership
SELECT * FROM products WHERE category IN ('Electronics', 'Books');

-- Range
SELECT * FROM orders WHERE amount BETWEEN 100 AND 500;
```

---

## :material-information-outline: Operator Precedence (high → low)

| Priority | Operators |
|----------|-----------|
| 1 (highest) | `~` (bitwise NOT), unary `-` |
| 2 | `*`, `/`, `%`, `DIV` |
| 3 | `+`, `-`, `\|\|` |
| 4 | `<<`, `>>` |
| 5 | `&` |
| 6 | `^` |
| 7 | `\|` |
| 8 | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `<=>`, `LIKE`, `RLIKE`, `IN`, `BETWEEN`, `IS` |
| 9 | `NOT` |
| 10 (lowest) | `AND`, `OR` |

Use parentheses to override default precedence and make intent explicit.
