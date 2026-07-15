# :material-file-tree: Subqueries

A subquery is a `SELECT` statement nested inside another SQL statement. Subqueries
filter rows, compute values, supply inline tables, or check existence — anywhere an
expression or table reference is valid.

---

## :material-sitemap: In This Section

| Page | Covers |
|------|--------|
| [Scalar Subqueries](scalar.md) | Single-value subqueries in `SELECT`, `WHERE`, `HAVING`, `CASE` |
| [IN / NOT IN](in_not_in.md) | Set membership tests and the NULL gotcha |
| [EXISTS / NOT EXISTS](exists.md) | Existence checks, semi-joins, anti-joins |
| [Correlated Subqueries](correlated.md) | Per-row subqueries referencing the outer query |
| [Derived Tables](derived_table.md) | Inline views in the `FROM` clause |
| [Subquery in HAVING](having_subquery.md) | Post-aggregation filters with subqueries |
| [Subquery vs JOIN vs CTE](subquery_vs_join.md) | When each approach is best |

---

## :material-code-tags: Subquery Types

| Type | Syntax | Returns |
|------|--------|---------|
| Scalar | `(SELECT single_val FROM ...)` | Exactly 1 row × 1 column |
| `IN` | `col IN (SELECT col FROM ...)` | A set of values |
| `EXISTS` | `EXISTS (SELECT 1 FROM ...)` | Boolean (row found or not) |
| Correlated | `WHERE col > (SELECT ... WHERE inner.id = outer.id)` | Any of the above, re-evaluated per outer row |
| Derived table | `FROM (SELECT ...) AS alias` | An inline table |

---

## :material-information-outline: Behavior

1. **Scalar subquery constraint**: must return at most one row and one column. A runtime error is raised if more than one row is returned.
2. **IN with NULL**: if the subquery result contains any `NULL` values, `NOT IN` may produce unexpected `UNKNOWN` results for every outer row — use `NOT EXISTS` instead to handle NULLs safely.
3. **Correlated subquery execution**: because the subquery references an outer column it is logically executed once per outer row. Spark's Catalyst optimizer decorrelates many correlated subqueries into joins automatically.
4. **Prefer EXISTS over IN**: `EXISTS` short-circuits on the first matching row and is NULL-safe, making it generally more robust than `IN` for subqueries that could return NULLs.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, 'Alice',   'US', '2024-01-15', 250.00),
  (2, 'Bob',     'CA', '2024-01-16', 120.00),
  (3, 'Alice',   'US', '2024-01-17', 300.00),
  (4, 'Charlie', 'US', '2024-01-18',  80.00),
  (5, 'Bob',     'CA', '2024-01-19', 450.00),
  (6, 'Alice',   'US', '2024-01-20', 175.00),
  (7, 'Dave',    NULL, '2024-01-21',  90.00)
AS orders(order_id, customer, country, order_date, amount);
```

### Example 1 — Scalar Subquery in SELECT and WHERE

```sql
SELECT
    order_id,
    customer,
    amount,
    (SELECT MAX(amount) FROM orders) AS max_order_amt
FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders);
-- AVG(amount) = (250+120+300+80+450+175+90)/7 ≈ 209.29
-- Result:
-- | order_id | customer | amount | max_order_amt |
-- |----------|----------|--------|---------------|
-- |        1 | Alice    | 250.00 |        450.00 |
-- |        3 | Alice    | 300.00 |        450.00 |
-- |        5 | Bob      | 450.00 |        450.00 |
```

### Example 2 — IN Subquery

Retrieve orders placed by customers who have ever ordered from the US:

```sql
SELECT order_id, customer, amount
FROM   orders
WHERE  customer IN (SELECT DISTINCT customer FROM orders WHERE country = 'US');
-- Result:
-- | order_id | customer | amount |
-- |----------|----------|--------|
-- |        1 | Alice    | 250.00 |
-- |        3 | Alice    | 300.00 |
-- |        4 | Charlie  |  80.00 |
-- |        6 | Alice    | 175.00 |
```

### Example 3 — NOT IN vs NOT EXISTS (NULL Gotcha)

```sql
-- NOT IN silently returns no rows when the subquery result contains NULL
SELECT DISTINCT customer FROM orders
WHERE country NOT IN (SELECT country FROM orders);
-- Returns 0 rows because NULL is in the subquery result (Dave's country)

-- NOT EXISTS is NULL-safe — always prefer this pattern
SELECT DISTINCT customer FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM orders i
    WHERE  i.customer = o.customer
    AND    i.country  = 'CA'
);
-- Result: customers who have never ordered from CA
-- | customer |
-- |----------|
-- | Alice    |
-- | Charlie  |
-- | Dave     |
```

### Example 4 — EXISTS Subquery

Find customers who have placed more than one order:

```sql
SELECT DISTINCT o1.customer
FROM   orders o1
WHERE  EXISTS (
    SELECT 1
    FROM   orders o2
    WHERE  o2.customer = o1.customer
    AND    o2.order_id <> o1.order_id
);
-- Result:
-- | customer |
-- |----------|
-- | Alice    |
-- | Bob      |
```

### Example 5 — Correlated Subquery (Orders Above Customer Average)

```sql
SELECT order_id, customer, amount
FROM   orders o
WHERE  amount > (
    SELECT AVG(amount)
    FROM   orders
    WHERE  customer = o.customer    -- references outer row
);
-- Result:
-- | order_id | customer | amount |
-- |----------|----------|--------|
-- |        3 | Alice    | 300.00 |  -- Alice avg = (250+300+175)/3 ≈ 241.67
-- |        5 | Bob      | 450.00 |  -- Bob avg   = (120+450)/2    = 285.00
```

### Example 6 — Derived Table in FROM

```sql
SELECT customer, total_spent
FROM (
    SELECT
        customer,
        SUM(amount) AS total_spent
    FROM   orders
    GROUP BY customer
) AS customer_totals
WHERE  total_spent > 200
ORDER BY total_spent DESC;
-- Result:
-- | customer | total_spent |
-- |----------|-------------|
-- | Alice    |      725.00 |
-- | Bob      |      570.00 |
```

### Example 7 — Subquery in HAVING

```sql
SELECT
    customer,
    SUM(amount) AS total_spent
FROM   orders
GROUP BY customer
HAVING SUM(amount) > (SELECT AVG(amount) FROM orders);
-- Filters groups whose total exceeds the overall single-order average (≈ 209.29)
-- Result:
-- | customer | total_spent |
-- |----------|-------------|
-- | Alice    |      725.00 |
-- | Bob      |      570.00 |
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommended Pattern | Performance Note |
|----------|---------------------|-----------------|
| Filter by a single computed threshold | Scalar subquery in `WHERE` | Evaluated once; very efficient |
| Check membership in a result set | `IN` subquery | Rewrite as a `JOIN` for large sets |
| Null-safe membership exclusion | `NOT EXISTS` | Always prefer over `NOT IN` with nullable columns |
| Check row existence in another table | `EXISTS` | Spark converts to a semi-join |
| Per-row comparison against a group aggregate | Correlated subquery | Spark decorrelates into a join when possible |
| Inline aggregation as a named table | Derived table in `FROM` | Equivalent to a CTE; consider CTEs for readability |
| Post-aggregation filter | Subquery in `HAVING` | Use a CTE for complex cases to aid readability |
