# :material-compare: Comparison Operators

Comparison operators evaluate two expressions and return `TRUE`, `FALSE`, or `NULL`.
They are the building blocks of `WHERE`, `HAVING`, `CASE WHEN`, and `JOIN ON` clauses.

---

## :material-code-tags: Syntax

| Operator | Meaning | NULL behaviour |
|----------|---------|----------------|
| `=` | Equal | Returns `NULL` if either side is `NULL` |
| `!=` / `<>` | Not equal | Returns `NULL` if either side is `NULL` |
| `>` | Greater than | Returns `NULL` if either side is `NULL` |
| `<` | Less than | Returns `NULL` if either side is `NULL` |
| `>=` | Greater than or equal | Returns `NULL` if either side is `NULL` |
| `<=` | Less than or equal | Returns `NULL` if either side is `NULL` |
| `<=>` | Null-safe equal | Returns `TRUE` when both are `NULL`; `FALSE` when only one is `NULL` |
| `BETWEEN a AND b` | Inclusive range `[a, b]` | Returns `NULL` if any operand is `NULL` |
| `LIKE pattern` | SQL wildcard match (`%`, `_`) | Returns `NULL` if either side is `NULL` |
| `ILIKE pattern` | Case-insensitive `LIKE` | Same NULL behaviour |
| `RLIKE pattern` | Java regex match | Returns `NULL` if either side is `NULL` |

---

## :material-information-outline: Behavior

1. Standard comparisons (`=`, `!=`, `>`, etc.) return `NULL` — not `FALSE` — when either operand is `NULL`. A `NULL` result is treated as falsy in `WHERE` and `HAVING`.
2. `<=>` (null-safe equality) is the only comparison that treats `NULL = NULL` as `TRUE` — use it in `JOIN ON` conditions that may involve nullable keys.
3. `BETWEEN a AND b` is inclusive: equivalent to `col >= a AND col <= b`. The order of `a` and `b` matters — `BETWEEN 10 AND 5` always returns `FALSE`.
4. `LIKE` uses `%` (any sequence of characters) and `_` (exactly one character). Use `\%` or `\_` to match literal `%` or `_`.
5. `RLIKE` accepts Java-compatible regular expressions.

---

## :material-flask-outline: Practical Examples

### Equality and inequality

```sql
SELECT * FROM orders
WHERE status = 'SHIPPED'
  AND region != 'APAC';
```

### Range filter with BETWEEN

```sql
-- Inclusive: amount >= 100 AND amount <= 500
SELECT order_id, amount
FROM orders
WHERE amount BETWEEN 100 AND 500;

-- Date range
SELECT * FROM events
WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31';
```

### NOT BETWEEN

```sql
SELECT * FROM transactions
WHERE amount NOT BETWEEN 0 AND 1000;  -- i.e., amount < 0 OR amount > 1000
```

### LIKE patterns

```sql
-- Starts with 'Pro'
SELECT * FROM products WHERE name LIKE 'Pro%';

-- Ends with '.com'
SELECT * FROM domains WHERE url LIKE '%.com';

-- Exactly 5 characters
SELECT * FROM codes WHERE code LIKE '_____';

-- Contains 'sale'
SELECT * FROM campaigns WHERE name LIKE '%sale%';
```

### Case-insensitive match (ILIKE)

```sql
-- Matches 'alice', 'Alice', 'ALICE', etc.
SELECT * FROM customers WHERE name ILIKE 'alice%';
```

### Regex match (RLIKE)

```sql
-- UK postcodes: letter(s) + digits + space + digit + letters
SELECT * FROM addresses WHERE postcode RLIKE '^[A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}$';

-- Email format validation
SELECT * FROM users WHERE email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$';
```

### Null-safe equality (<=>)

```sql
-- Standard = returns NULL (not FALSE) when comparing to NULL
SELECT 1 = NULL;    -- NULL

-- <=> returns TRUE when both sides are NULL
SELECT NULL <=> NULL;  -- TRUE
SELECT NULL <=> 1;     -- FALSE

-- Use in JOIN to correctly match NULL keys
SELECT o.order_id, c.name
FROM orders AS o
JOIN customers AS c
    ON o.customer_id <=> c.customer_id;  -- matches even when both are NULL
```

### Chained comparisons in CASE WHEN

```sql
SELECT
    amount,
    CASE
        WHEN amount >= 1000 THEN 'Large'
        WHEN amount >= 500  THEN 'Medium'
        WHEN amount >= 100  THEN 'Small'
        ELSE                     'Micro'
    END AS order_size
FROM orders;
```

### Compare computed expressions

```sql
SELECT
    product_id,
    unit_price,
    discount_pct,
    unit_price * (1 - discount_pct / 100.0) AS net_price
FROM products
WHERE unit_price * (1 - discount_pct / 100.0) >= 50;
```

### NOT LIKE

```sql
-- Exclude test accounts
SELECT * FROM users WHERE email NOT LIKE '%@test.%'
                      AND email NOT LIKE '%@example.%';
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Operator |
|----------|---------|
| Exact value match | `=` |
| Exclude a value | `!=` or `<>` |
| Numeric / date range | `BETWEEN a AND b` |
| Nullable column equality in JOIN | `<=>` |
| Prefix / suffix / contains text | `LIKE` with `%` |
| Case-insensitive text match | `ILIKE` |
| Complex pattern match | `RLIKE` (Java regex) |
| Null-safe inequality | `NOT (a <=> b)` or `IS DISTINCT FROM` |
