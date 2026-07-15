# :material-numeric-1-box-outline: Scalar Subqueries

A scalar subquery returns exactly **one row and one column**. It can appear anywhere a
single value is expected: `SELECT` column list, `WHERE`, `HAVING`, `CASE WHEN`, `ORDER BY`,
and even `JOIN ON` conditions.

---

## :material-code-tags: Syntax

```sql
-- In SELECT list (computed column)
SELECT col, (SELECT AGG(col2) FROM t2 WHERE ...) AS computed
FROM t1;

-- In WHERE (filter against a threshold)
SELECT * FROM t1
WHERE col > (SELECT AGG(col2) FROM t2);

-- In HAVING
SELECT group_col, AGG(col)
FROM t1
GROUP BY group_col
HAVING AGG(col) > (SELECT AGG(col2) FROM t2);

-- In CASE WHEN
SELECT
    col,
    CASE WHEN col > (SELECT AVG(col) FROM t1) THEN 'above' ELSE 'below' END AS vs_avg
FROM t1;
```

!!! warning "Single-row constraint"
    A scalar subquery **must** return at most one row. If it returns more than one row
    at runtime, Spark raises:
    `RuntimeException: more than one row returned by a subquery used as an expression`.
    Always apply an aggregate (`MAX`, `MIN`, `AVG`, `COUNT`, `SUM`) or `LIMIT 1` to
    guarantee a single row.

---

## :material-information-outline: Behavior

1. A scalar subquery is evaluated **once** by the optimizer when it has no reference to the outer query (uncorrelated). The result is treated as a constant.
2. When it references an outer column (correlated), it is logically re-evaluated per outer row — Spark's Catalyst optimizer attempts to decorrelate it into a join.
3. If the subquery returns **zero rows**, the result is `NULL`. Handle this with `COALESCE` if needed.
4. Scalar subqueries in the `SELECT` list add one extra computation per column — use sparingly for columns that appear in every row.

---

## :material-flask-outline: Practical Examples

### Compare each row against the overall average

```sql
SELECT
    order_id,
    customer_id,
    amount,
    ROUND((SELECT AVG(amount) FROM orders), 2)              AS overall_avg,
    ROUND(amount - (SELECT AVG(amount) FROM orders), 2)     AS diff_from_avg
FROM orders
ORDER BY diff_from_avg DESC;
```

### Filter above the global maximum of another table

```sql
-- Customers whose balance exceeds the highest single order amount
SELECT customer_id, balance
FROM accounts
WHERE balance > (SELECT MAX(amount) FROM orders);
```

### Threshold from a config or lookup table

```sql
-- Threshold stored in a config table
SELECT product_id, stock_qty
FROM inventory
WHERE stock_qty < (
    SELECT CAST(config_value AS INT)
    FROM app_config
    WHERE config_key = 'low_stock_threshold'
);
```

### Scalar subquery in CASE WHEN

```sql
SELECT
    customer_id,
    SUM(amount) AS total_spent,
    CASE
        WHEN SUM(amount) >= (SELECT PERCENTILE(total, 0.9) FROM customer_summary)
        THEN 'VIP'
        WHEN SUM(amount) >= (SELECT PERCENTILE(total, 0.5) FROM customer_summary)
        THEN 'Regular'
        ELSE 'Low value'
    END AS segment
FROM orders
GROUP BY customer_id;
```

### NULL handling when subquery returns no rows

```sql
-- If no order exists for that region, MAX returns NULL — COALESCE replaces it
SELECT
    region,
    SUM(amount)                                                         AS region_total,
    COALESCE((SELECT MAX(amount) FROM orders WHERE region = o.region), 0) AS region_max
FROM orders AS o
GROUP BY region;
```

### Scalar subquery in ORDER BY

```sql
-- Sort products by how far their price is from the category average
SELECT
    product_id,
    name,
    category,
    price
FROM products AS p
ORDER BY ABS(price - (
    SELECT AVG(price)
    FROM products
    WHERE category = p.category
)) DESC;
```

### Row count from a related table

```sql
SELECT
    c.customer_id,
    c.name,
    (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) AS order_count
FROM customers AS c
WHERE (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) > 5;
```

!!! tip "Prefer JOIN for large tables"
    The correlated scalar subquery above re-executes per customer row.
    For large tables, rewrite using `LEFT JOIN` and `GROUP BY` — Spark can optimise
    joins far more efficiently than repeated subquery executions.

---

## :material-lightbulb-outline: When to Use Scalar Subqueries

| Scenario | Pattern |
|----------|---------|
| Filter against a single computed threshold | `WHERE col > (SELECT AGG(...) FROM ...)` |
| Add a global benchmark column to every row | Scalar in `SELECT` list |
| Segment rows using a percentile boundary | Scalar in `CASE WHEN` |
| Threshold comes from a config/lookup table | Scalar in `WHERE` or `CASE WHEN` |
| Sort by distance from group average | Scalar in `ORDER BY` |
