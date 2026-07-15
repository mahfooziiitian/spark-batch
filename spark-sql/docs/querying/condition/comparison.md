# :material-compare: Comparison Conditions

Comparison predicates test equality, ordering, or membership between values.
They are the most common predicate type — appearing in `WHERE`, `HAVING`, `JOIN … ON`, and `CASE WHEN`.

---

## :material-pin: Operator Reference

| Operator | Syntax | Notes |
|----------|--------|-------|
| `=` | `a = b` | Equality — NULL = anything → NULL |
| `!=` / `<>` | `a != b` | Inequality |
| `>` `>=` `<` `<=` | `a >= 10` | Ordering — works on numbers, strings, dates |
| `<=>` | `a <=> b` | NULL-safe equality — NULL <=> NULL → TRUE |
| `BETWEEN … AND` | `x BETWEEN 1 AND 5` | Inclusive on both ends |
| `NOT BETWEEN` | `x NOT BETWEEN 1 AND 5` | Outside range |
| `IN (…)` | `id IN (1, 2, 3)` | Set membership |
| `NOT IN (…)` | `id NOT IN (1, 2, 3)` | Excludes set — **dangerous with NULLs** |
| `IN (subquery)` | `id IN (SELECT …)` | Subquery membership |
| `IS DISTINCT FROM` | `a IS DISTINCT FROM b` | NULL-aware inequality |
| `IS NOT DISTINCT FROM` | `a IS NOT DISTINCT FROM b` | NULL-aware equality (alias for `<=>`) |

---

## :material-table: Behavior with NULLs

| Expression | Result |
|------------|--------|
| `NULL = NULL` | NULL (not TRUE) |
| `NULL != NULL` | NULL |
| `NULL <=> NULL` | TRUE |
| `1 <=> NULL` | FALSE |
| `NULL IS DISTINCT FROM NULL` | FALSE |
| `1 IS DISTINCT FROM NULL` | TRUE |
| `NULL IN (1, 2, NULL)` | NULL |
| `1 NOT IN (2, NULL)` | NULL (not TRUE) |

!!! warning "NOT IN NULL trap"
    `id NOT IN (SELECT id FROM blocked WHERE id IS NULL)` → returns **zero rows**.
    Always filter NULLs from the subquery: `WHERE id IS NOT NULL`.

---

## :material-flask-outline: Examples

### Equality and inequality

```sql
SELECT * FROM orders WHERE status = 'shipped';
SELECT * FROM orders WHERE status != 'cancelled';
```

### Range filter with BETWEEN

```sql
-- Inclusive: duration = 60 and duration = 300 are both included
SELECT * FROM sessions
WHERE duration BETWEEN 60 AND 300;

-- Date range
SELECT * FROM events
WHERE event_date BETWEEN DATE '2024-01-01' AND DATE '2024-03-31';
```

### Membership with IN

```sql
SELECT * FROM users
WHERE country IN ('US', 'CA', 'UK');

-- Subquery version
SELECT * FROM orders
WHERE customer_id IN (SELECT customer_id FROM vip_customers);
```

### Safe NOT IN with NULL guard

```sql
-- Unsafe — returns no rows if blocked list has any NULL
SELECT * FROM users
WHERE user_id NOT IN (SELECT user_id FROM blocked);

-- Safe — guard against NULLs in subquery
SELECT * FROM users
WHERE user_id NOT IN (
    SELECT user_id FROM blocked WHERE user_id IS NOT NULL
);

-- Safest — NOT EXISTS is immune to NULL
SELECT u.* FROM users u
WHERE NOT EXISTS (
    SELECT 1 FROM blocked b WHERE b.user_id = u.user_id
);
```

### NULL-safe equality in joins

```sql
-- Standard join misses NULL = NULL matches
SELECT * FROM a JOIN b ON a.device_id = b.device_id;

-- NULL-safe: NULL <=> NULL → TRUE
SELECT * FROM a JOIN b ON a.device_id <=> b.device_id;
```

### IS DISTINCT FROM — NULL-aware inequality

```sql
-- Detect any change, including NULL ↔ non-NULL transitions
SELECT *
FROM dim_customer
WHERE new_city IS DISTINCT FROM old_city;
```

### Numeric ordering

```sql
SELECT
    product_id,
    price,
    CASE
        WHEN price < 10          THEN 'budget'
        WHEN price BETWEEN 10 AND 50 THEN 'mid'
        ELSE 'premium'
    END AS tier
FROM products;
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `WHERE col = NULL` | Always returns NULL (no rows) | `WHERE col IS NULL` |
| `NOT IN (subquery)` with NULLs | Returns no rows | Add `WHERE col IS NOT NULL` to subquery |
| `BETWEEN` on timestamps | Misses rows after midnight of end date | Use `>= start AND < next_day` |
| String vs integer compare | Implicit cast — may cause full scan | Explicit `CAST(col AS INT)` |

---

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Equality (no NULLs) | `=` |
| Equality (NULLs possible) | `<=>` or `IS NOT DISTINCT FROM` |
| Range check | `BETWEEN` |
| Lookup list | `IN (…)` |
| Exclusion list (safe) | `NOT EXISTS` |
| Change detection (with NULLs) | `IS DISTINCT FROM` |

!!! tip
    Prefer `>= start AND < end` over `BETWEEN` for timestamp ranges — avoids ambiguity at midnight boundaries.


Comparison predicates test equality or ordering between values.

---

## :material-pin: Operators

| Operator | Example | Notes |
|----------|---------|-------|
| `=` | `a = b` | Equality |
| `!=` or `<>` | `a != b` | Inequality |
| `>` `>=` `<` `<=` | `a >= 10` | Ordering |
| `<=>` | `a <=> b` | NULL-safe equality |
| `BETWEEN` | `x BETWEEN 1 AND 5` | Inclusive range |
| `IN` | `id IN (1,2,3)` | Membership |

---

## :material-magnify: Behavior Notes

1. **`BETWEEN` is inclusive** — `x BETWEEN 1 AND 5` includes 1 and 5.
2. **`IN` with NULLs** — If the list (or subquery) contains NULL, `NOT IN` can
   return no rows. Prefer `NOT EXISTS` for anti-joins.
3. **`<=>` for NULLs** — `NULL <=> NULL` is TRUE; `NULL = NULL` is NULL.

---

## :material-flask-outline: Practical Examples

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

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Equality matches | `=` or `<=>` (if NULLs possible) |
| Range checks | `BETWEEN` |
| Lookup lists | `IN (...)` |
| Safe NULL comparisons | `<=>` |

---

> **Tip:** Use explicit `CAST()` when comparing strings to numbers to avoid
> unexpected type coercion.
