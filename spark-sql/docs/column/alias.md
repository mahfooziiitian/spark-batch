# :material-label-outline: Column Aliases

An alias assigns a readable name to a column expression, computed value, or
subquery result. Aliases appear in query output, are referenced by downstream
`ORDER BY` clauses, and define the column names of CTEs and derived tables.

---

## :material-code-tags: Syntax

```sql
-- Named alias with AS keyword (preferred)
SELECT amount * 1.1 AS amount_with_tax FROM orders;

-- Alias without AS (also valid)
SELECT amount * 1.1 amount_with_tax FROM orders;

-- Quoted alias (for spaces, special characters, reserved words)
SELECT amount * 1.1 AS `amount with tax` FROM orders;

-- Table alias
SELECT o.order_id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id;

-- CTE alias
WITH totals AS (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id)
SELECT * FROM totals;
```

---

## :material-information-outline: Behavior

1. Column aliases defined in `SELECT` are **not visible** in `WHERE` or `HAVING` — both evaluate before `SELECT`. Use a CTE or subquery to filter on an alias.
2. Aliases **are** visible in `ORDER BY` — `ORDER BY amount_with_tax DESC` works.
3. `GROUP BY` in Spark SQL (non-ANSI mode) can reference aliases defined in the same `SELECT`; in ANSI mode it cannot — use position or repeat the expression.
4. Backtick quoting `` ` `` allows aliases with spaces, dots, or reserved-word names.
5. `AS` is optional but always include it for readability.
6. Table aliases declared in `FROM` / `JOIN` must be used consistently — once aliased, the original name is no longer valid in the same query.

---

## :material-flask-outline: Practical Examples

### Simple expression alias

```sql
SELECT
    order_id,
    unit_price * quantity                           AS line_subtotal,
    unit_price * quantity * (1 + tax_rate)          AS line_total,
    unit_price * quantity * discount_pct / 100.0    AS line_discount
FROM order_lines;
```

### Alias in ORDER BY

```sql
SELECT
    customer_id,
    SUM(amount) AS lifetime_value
FROM orders
GROUP BY customer_id
ORDER BY lifetime_value DESC   -- alias visible here
LIMIT 20;
```

### Alias NOT visible in WHERE — use a CTE

```sql
-- BAD: AnalysisException: alias not resolvable in WHERE
SELECT customer_id, SUM(amount) AS total
FROM orders
WHERE total > 1000    -- total is not defined yet at WHERE evaluation
GROUP BY customer_id;

-- GOOD: Fix 1: use HAVING (for aggregates)
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 1000;

-- GOOD: Fix 2: wrap in a CTE or subquery
WITH totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
)
SELECT * FROM totals WHERE total > 1000;
```

### Quoted alias for special characters

```sql
SELECT
    order_id                AS `Order ID`,
    customer_name           AS `Customer Name`,
    amount * 1.1            AS `Amount (inc. Tax)`,
    order_date              AS `Order Date`
FROM orders;
```

### Table alias to disambiguate columns

```sql
SELECT
    o.order_id,
    o.amount,
    c.name      AS customer_name,
    c.email     AS customer_email,
    p.name      AS product_name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id
JOIN products  AS p ON o.product_id  = p.product_id;
```

### Alias in GROUP BY (Spark non-ANSI mode)

```sql
-- Works in Spark SQL (non-ANSI): alias in GROUP BY
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    SUM(amount)                     AS monthly_revenue
FROM orders
GROUP BY order_month      -- alias reference (Spark extension)
ORDER BY order_month;

-- ANSI-safe version: repeat the expression
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    SUM(amount)                     AS monthly_revenue
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY order_month;
```

### CTE alias used across multiple queries

```sql
WITH customer_ltv AS (
    SELECT
        customer_id,
        SUM(amount)       AS lifetime_value,
        COUNT(*)          AS order_count,
        AVG(amount)       AS avg_order_value
    FROM orders
    GROUP BY customer_id
)
SELECT
    cl.customer_id,
    cl.lifetime_value,
    c.name,
    c.segment
FROM customer_ltv AS cl
JOIN customers    AS c  ON cl.customer_id = c.customer_id
WHERE cl.lifetime_value > 500;
```

### Alias in window function reference

```sql
SELECT
    order_id,
    customer_id,
    amount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;
-- running_total can be referenced in ORDER BY
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Readable computed column name | `expression AS alias` |
| Alias with spaces or reserved words | `` expression AS `alias name` `` |
| Shorten long table name in joins | `long_table_name AS t` |
| Filter on a computed column | Wrap in CTE, then `WHERE alias` |
| Aggregate alias in ORDER BY | `ORDER BY alias_name` directly |
| GROUP BY computed expression | Repeat expression or use alias (non-ANSI) |

!!! tip "Always use AS"
    `SELECT amount total` (without `AS`) is valid but ambiguous to readers.
    Always write `SELECT amount AS total` — it takes one word and removes all ambiguity.
