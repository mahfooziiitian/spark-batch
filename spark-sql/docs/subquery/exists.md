# :material-check-circle-outline: EXISTS / NOT EXISTS Subqueries

`EXISTS` returns `TRUE` if a subquery produces at least one row; `NOT EXISTS` returns
`TRUE` if it produces no rows. Both are NULL-safe and are the preferred alternative to
`IN`/`NOT IN` whenever the subquery column may contain `NULL`.

---

## :material-code-tags: Syntax

```sql
-- EXISTS: keep outer rows that have at least one matching inner row
SELECT * FROM outer_table AS o
WHERE EXISTS (
    SELECT 1
    FROM inner_table AS i
    WHERE i.fk_col = o.pk_col
      AND i.filter_col = 'value'
);

-- NOT EXISTS: keep outer rows with NO matching inner row
SELECT * FROM outer_table AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM inner_table AS i
    WHERE i.fk_col = o.pk_col
);
```

!!! tip "Use `SELECT 1`"
    The `SELECT` list inside an `EXISTS` subquery is irrelevant — use `SELECT 1` or
    `SELECT NULL` by convention to signal that you only care about row existence.

---

## :material-information-outline: Behavior

1. Catalyst rewrites `EXISTS` into a **semi-join** and `NOT EXISTS` into an **anti-join** — both are optimised join operations, not row-by-row lookups.
2. Both are **NULL-safe**: unlike `NOT IN`, `NOT EXISTS` correctly handles `NULL` values in the subquery result.
3. `EXISTS` **short-circuits** logically — as soon as one matching row is found, the subquery stops. In practice Spark's join implementation handles this at the plan level.
4. Correlated `EXISTS` subqueries (referencing outer columns) are decorrelated by Catalyst into efficient joins when possible.
5. `EXISTS` with a `LIMIT 1` inside is redundant — omit it; Catalyst already knows to stop after the first match.

---

## :material-flask-outline: Practical Examples

### EXISTS: customers with at least one order

```sql
SELECT c.customer_id, c.name, c.email
FROM customers AS c
WHERE EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
);
```

### NOT EXISTS: customers who have never ordered

```sql
SELECT c.customer_id, c.name, c.email
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
);
```

### NOT EXISTS: NULL-safe country exclusion (vs NOT IN bug)

```sql
-- ✅ NULL-safe: even if country is NULL in blocked_countries, NOT EXISTS works correctly
SELECT c.customer_id, c.name
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM blocked_countries AS bc
    WHERE bc.country = c.country
);
```

### EXISTS with additional filter conditions

```sql
-- Customers who placed a large order in 2024
SELECT c.customer_id, c.name
FROM customers AS c
WHERE EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
      AND o.amount      > 500
      AND o.order_date >= '2024-01-01'
);
```

### NOT EXISTS: products never sold

```sql
SELECT p.product_id, p.name, p.category
FROM products AS p
WHERE NOT EXISTS (
    SELECT 1
    FROM order_lines AS ol
    WHERE ol.product_id = p.product_id
);
```

### NOT EXISTS: find gaps (dates with no orders)

```sql
-- Date spine CTE; flag dates with no orders
WITH RECURSIVE date_spine AS (
    SELECT DATE('2024-01-01') AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt) FROM date_spine WHERE dt < DATE('2024-01-31')
)
SELECT dt AS missing_date
FROM date_spine AS ds
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.order_date = ds.dt
);
```

### EXISTS to validate referential integrity

```sql
-- Orphaned order lines whose order_id no longer exists in orders
SELECT ol.line_id, ol.order_id, ol.product_id
FROM order_lines AS ol
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.order_id = ol.order_id
);
```

### EXISTS in CASE WHEN

```sql
-- Tag each customer based on whether they have active subscriptions
SELECT
    c.customer_id,
    c.name,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM subscriptions s
            WHERE s.customer_id = c.customer_id
              AND s.status = 'ACTIVE'
        ) THEN 'Subscriber'
        ELSE 'Non-subscriber'
    END AS subscription_status
FROM customers AS c;
```

### Double NOT EXISTS: customers in A but not B and not C

```sql
-- Customers in the EU segment who have neither complained nor churned
SELECT c.customer_id, c.name
FROM customers AS c
WHERE c.region = 'EU'
  AND NOT EXISTS (
      SELECT 1 FROM complaints cp WHERE cp.customer_id = c.customer_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM churned_customers ch WHERE ch.customer_id = c.customer_id
  );
```

---

## :material-swap-horizontal: EXISTS vs IN vs LEFT JOIN Anti-Pattern

```sql
-- Three equivalent ways to find customers with no orders:

-- EXISTS (recommended — NULL-safe, readable)
SELECT c.customer_id FROM customers AS c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- NOT IN (dangerous if orders.customer_id is nullable)
SELECT customer_id FROM customers
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders);

-- LEFT JOIN + IS NULL (explicit, optimizer-friendly for large tables)
SELECT c.customer_id
FROM customers AS c
LEFT JOIN orders AS o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|---------------|
| Check that at least one related row exists | `EXISTS` |
| Exclude rows with any match (NULL-safe) | `NOT EXISTS` |
| Replace `NOT IN` on a nullable column | Always use `NOT EXISTS` |
| Find orphaned / unmatched records | `NOT EXISTS` or `LEFT JOIN ... IS NULL` |
| Tag rows based on presence in another table | `EXISTS` in `CASE WHEN` |
| Find date/sequence gaps | `NOT EXISTS` against a spine CTE |
