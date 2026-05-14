# :material-arrow-decision: Correlated Subqueries

A correlated subquery references one or more columns from the **outer query**. It is
logically re-evaluated for each row of the outer query, enabling per-row comparisons
against group-level aggregates or existence checks in related tables.

---

## :material-code-tags: Syntax

```sql
-- Correlated scalar: compare each row against its own group average
SELECT col1, col2
FROM outer_table AS o
WHERE col2 > (
    SELECT AVG(col2)
    FROM outer_table
    WHERE group_col = o.group_col   -- references outer alias o
);

-- Correlated EXISTS: filter outer rows by related-table criteria
SELECT * FROM outer_table AS o
WHERE EXISTS (
    SELECT 1
    FROM related_table AS r
    WHERE r.fk = o.pk
      AND r.status = 'ACTIVE'
);

-- Correlated scalar in SELECT list
SELECT
    o.col1,
    (SELECT MAX(r.col2) FROM related_table r WHERE r.fk = o.pk) AS max_related
FROM outer_table AS o;
```

---

## :material-information-outline: Behavior

1. Spark's Catalyst optimizer **decorrelates** most correlated subqueries into efficient joins automatically — inspect `EXPLAIN EXTENDED` to verify.
2. If decorrelation fails (e.g., complex expressions), the subquery is executed once per outer row — this is expensive on large tables.
3. Correlated subqueries in `WHERE`, `HAVING`, and `SELECT` are all supported.
4. A correlated scalar subquery must still return at most one row per outer row.
5. Use `EXPLAIN` to confirm Catalyst turned the subquery into a `LeftSemi`, `LeftAnti`, or `LeftOuter` join — if you still see `Subquery` in the plan, consider rewriting as an explicit join.

---

## :material-flask-outline: Practical Examples

### Orders above that customer's own average

```sql
SELECT order_id, customer_id, amount
FROM orders AS o
WHERE amount > (
    SELECT AVG(amount)
    FROM orders
    WHERE customer_id = o.customer_id
);
-- Each customer's orders are filtered against their personal average, not the global average
```

### Top order per customer (correlated filter)

```sql
SELECT order_id, customer_id, amount, order_date
FROM orders AS o
WHERE amount = (
    SELECT MAX(amount)
    FROM orders
    WHERE customer_id = o.customer_id
);
-- Returns the row(s) with the highest amount for each customer
```

### Products priced above their category average

```sql
SELECT product_id, name, category, price
FROM products AS p
WHERE price > (
    SELECT AVG(price)
    FROM products
    WHERE category = p.category
);
```

### Employees earning above their department median

```sql
SELECT employee_id, name, department, salary
FROM employees AS e
WHERE salary > (
    SELECT PERCENTILE(salary, 0.5)
    FROM employees
    WHERE department = e.department
);
```

### Correlated EXISTS: customers active in the last 30 days

```sql
SELECT c.customer_id, c.name, c.segment
FROM customers AS c
WHERE EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= DATEADD(DAY, -30, CURRENT_DATE())
);
```

### Correlated scalar in SELECT: last order date per customer

```sql
SELECT
    c.customer_id,
    c.name,
    (
        SELECT MAX(order_date)
        FROM orders AS o
        WHERE o.customer_id = c.customer_id
    ) AS last_order_date
FROM customers AS c;
```

### Correlated NOT EXISTS: customers with no recent activity

```sql
SELECT c.customer_id, c.name, c.email
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= DATEADD(DAY, -180, CURRENT_DATE())
);
```

### Correlated subquery in HAVING

```sql
-- Regions whose total revenue exceeds that region's previous-year revenue
SELECT
    region,
    SUM(amount) AS this_year_revenue
FROM orders
WHERE YEAR(order_date) = 2024
GROUP BY region
HAVING SUM(amount) > (
    SELECT SUM(amount)
    FROM orders
    WHERE YEAR(order_date) = 2023
      AND region = orders.region   -- correlates on the outer GROUP BY key
);
```

### Rewrite correlated scalar as a JOIN (for performance)

```sql
-- ❌ Correlated scalar — may not decorrelate on complex expressions
SELECT
    o.order_id,
    o.amount,
    (SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id) AS customer_avg
FROM orders AS o;

-- ✅ Explicit join — always efficient
SELECT
    o.order_id,
    o.amount,
    ca.customer_avg
FROM orders AS o
JOIN (
    SELECT customer_id, AVG(amount) AS customer_avg
    FROM orders
    GROUP BY customer_id
) AS ca ON o.customer_id = ca.customer_id;
```

---

## :material-lightbulb-outline: When to Use Correlated Subqueries

| Scenario | Recommendation |
|----------|---------------|
| Filter rows against their own group aggregate | Correlated scalar in `WHERE` |
| Find the max/min row per group | Correlated scalar in `WHERE` (or `ROW_NUMBER` window function) |
| Check related-table conditions per row | Correlated `EXISTS` / `NOT EXISTS` |
| Add a per-row lookup column | Correlated scalar in `SELECT` list |
| Complex condition that Catalyst fails to decorrelate | Rewrite as explicit `JOIN` |

!!! tip "Verify decorrelation with EXPLAIN"
    Run `EXPLAIN EXTENDED` and look for `LeftSemi`, `LeftAnti`, or `LeftOuter` join nodes.
    If you still see a `Subquery` node, the optimizer could not decorrelate — rewrite as
    an explicit join or a window function for large tables.
