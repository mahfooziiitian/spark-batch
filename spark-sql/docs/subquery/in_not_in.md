# :material-format-list-bulleted: IN / NOT IN Subqueries

`IN` and `NOT IN` test whether a column value belongs to a set returned by a subquery.
They are concise but carry a critical NULL trap that makes `NOT IN` unsafe whenever the
subquery can return `NULL` values.

---

## :material-code-tags: Syntax

```sql
-- IN: rows where col matches any value in the subquery result
SELECT * FROM t1
WHERE col IN (SELECT col2 FROM t2 WHERE ...);

-- NOT IN: rows where col matches none of the values
SELECT * FROM t1
WHERE col NOT IN (SELECT col2 FROM t2 WHERE ...);

-- IN with a literal list (not a subquery)
SELECT * FROM t1
WHERE status IN ('ACTIVE', 'PENDING', 'TRIAL');

-- Multi-column IN (row value constructor)
SELECT * FROM t1
WHERE (col1, col2) IN (SELECT col_a, col_b FROM t2);
```

---

## :material-information-outline: Behavior

1. `IN (subquery)` is rewritten by Catalyst into a **semi-join** — only the outer table rows that have a match are kept.
2. `NOT IN (subquery)` is rewritten into an **anti-join** — outer rows with no match are kept.
3. **NULL trap**: if the subquery result contains any `NULL`, `NOT IN` returns `UNKNOWN` (treated as `FALSE`) for **every outer row** — the query returns zero rows. This is the most common `NOT IN` bug.
4. `IN` with a **literal list** is not affected by the NULL trap — use it freely for small known sets.
5. For large subquery result sets, a `JOIN` is often more efficient because the optimizer has more join strategy options.
6. `IN` with more than `spark.sql.optimizer.inSetConversionThreshold` (default 10) literal values is automatically converted to a `HashSet` lookup.

!!! warning "NOT IN + NULL = silent empty result"
    Always check whether the `NOT IN` subquery column is nullable.
    If it is, use `NOT EXISTS` instead — it is NULL-safe.

---

## :material-flask-outline: Practical Examples

### IN: orders from high-value customers

```sql
SELECT order_id, customer_id, amount
FROM orders
WHERE customer_id IN (
    SELECT customer_id
    FROM customers
    WHERE segment = 'VIP'
);
```

### IN: products in active categories

```sql
SELECT product_id, name, price
FROM products
WHERE category_id IN (
    SELECT category_id
    FROM categories
    WHERE is_active = TRUE
);
```

### NOT IN (safe — subquery column is NOT NULL)

```sql
-- region is NOT NULL in all rows — NOT IN is safe here
SELECT order_id, amount
FROM orders
WHERE region NOT IN (
    SELECT region
    FROM blacklisted_regions
);
```

### NOT IN NULL trap (and the fix)

```sql
-- ❌ country can be NULL — returns ZERO rows
SELECT customer_id
FROM customers
WHERE country NOT IN (SELECT country FROM blocked_countries);

-- ✅ Fix 1: filter NULLs from the subquery
SELECT customer_id
FROM customers
WHERE country NOT IN (
    SELECT country FROM blocked_countries WHERE country IS NOT NULL
);

-- ✅ Fix 2: use NOT EXISTS (always NULL-safe)
SELECT c.customer_id
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1 FROM blocked_countries bc WHERE bc.country = c.country
);
```

### Multi-column IN

```sql
-- Find orders whose (customer_id, product_id) pair was in last month's promotions
SELECT order_id, customer_id, product_id, amount
FROM orders
WHERE (customer_id, product_id) IN (
    SELECT customer_id, product_id
    FROM promotions
    WHERE promo_month = '2024-05'
);
```

### IN with literal list

```sql
SELECT order_id, status, amount
FROM orders
WHERE status IN ('PENDING', 'PROCESSING', 'ON_HOLD');
```

### NOT IN to find missing records

```sql
-- Customers who have never placed an order
-- Safe because orders.customer_id has a NOT NULL constraint
SELECT customer_id, name
FROM customers
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id FROM orders
);
```

### Large IN list — let Spark use HashSet

```sql
-- Over 10 values — Catalyst converts to in-memory HashSet automatically
SELECT * FROM products
WHERE category IN (
    'Electronics', 'Books', 'Clothing', 'Home', 'Sports',
    'Toys', 'Grocery', 'Automotive', 'Garden', 'Office', 'Pet'
);
```

---

## :material-swap-horizontal: IN vs EXISTS vs JOIN

| Aspect | `IN (subquery)` | `EXISTS (subquery)` | `JOIN` |
|--------|----------------|-------------------|-------|
| NULL safety (`NOT`) | Unsafe if subquery returns NULL | Always safe | Safe with `LEFT JOIN ... IS NULL` |
| Short-circuit | No — full subquery evaluated | Yes — stops at first match | No |
| Duplicate handling | Automatically deduped | N/A | Must deduplicate join side |
| Readability | Very concise | Slightly more verbose | Most explicit |
| Performance (large sets) | May be slower than JOIN | Efficient (semi-join) | Most optimizer flexibility |

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|---------------|
| Small known set of values | `IN ('val1', 'val2', ...)` literal list |
| Filter by a small subquery result | `IN (subquery)` |
| Exclusion where subquery is NOT NULL guaranteed | `NOT IN (subquery)` |
| Exclusion where subquery column is nullable | `NOT EXISTS` |
| Large subquery result set | Rewrite as `JOIN` |
| Multi-column membership test | `(col1, col2) IN (SELECT ...)` |
