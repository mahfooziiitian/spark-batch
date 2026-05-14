# :material-table-arrow-right: Derived Tables (Inline Views)

A derived table is a subquery in the `FROM` clause, given an alias and treated as a
temporary inline table. It is the oldest form of a named subquery and is semantically
identical to a CTE — use CTEs for readability, derived tables when nesting is
intentional or the query is a one-liner.

---

## :material-code-tags: Syntax

```sql
SELECT outer_col, ...
FROM (
    SELECT col1, col2, AGG(col3) AS agg_col
    FROM base_table
    WHERE ...
    GROUP BY col1, col2
) AS derived_alias
WHERE derived_alias.agg_col > threshold
ORDER BY outer_col;
```

Rules:
- The alias after `)` is **required** in Spark SQL.
- Column names from the inner `SELECT` are the column names of the derived table.
- Derived tables can be nested (though CTEs are more readable for deep nesting).
- Derived tables in the `FROM` clause can be joined to other tables.

---

## :material-information-outline: Behavior

1. The optimizer **inlines** derived tables exactly as it does CTEs — there is no automatic materialization boundary.
2. Filters in the outer `WHERE` are pushed into the derived table when possible (predicate pushdown through the view).
3. Column pruning applies — only columns referenced in the outer query are read from the inner subquery.
4. Derived tables cannot be referenced more than once in the same query (unlike CTEs). Use a CTE when you need to reference the result set in multiple places.

---

## :material-flask-outline: Practical Examples

### Post-aggregation filter

```sql
SELECT region, total_revenue
FROM (
    SELECT
        region,
        SUM(amount) AS total_revenue
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY region
) AS regional_totals
WHERE total_revenue > 100000
ORDER BY total_revenue DESC;
```

### Rank inside a derived table, filter outside

```sql
SELECT customer_id, total_spent, spend_rank
FROM (
    SELECT
        customer_id,
        SUM(amount)                                     AS total_spent,
        RANK() OVER (ORDER BY SUM(amount) DESC)         AS spend_rank
    FROM orders
    GROUP BY customer_id
) AS ranked
WHERE spend_rank <= 10;
```

### Join two derived tables

```sql
SELECT
    r.region,
    r.total_revenue,
    c.customer_count,
    ROUND(r.total_revenue / c.customer_count, 2) AS revenue_per_customer
FROM (
    SELECT region, SUM(amount) AS total_revenue
    FROM orders
    GROUP BY region
) AS r
JOIN (
    SELECT region, COUNT(DISTINCT customer_id) AS customer_count
    FROM orders
    GROUP BY region
) AS c ON r.region = c.region
ORDER BY revenue_per_customer DESC;
```

### Derived table to avoid re-aggregating

```sql
-- Calculate category share without computing category totals twice
SELECT
    cat.category,
    cat.category_revenue,
    ROUND(cat.category_revenue / tot.grand_total * 100, 2) AS pct_of_total
FROM (
    SELECT category, SUM(amount) AS category_revenue
    FROM orders JOIN products USING (product_id)
    GROUP BY category
) AS cat
CROSS JOIN (
    SELECT SUM(amount) AS grand_total FROM orders
) AS tot
ORDER BY pct_of_total DESC;
```

### Derived table with LIMIT for top-N before join

```sql
-- Join only the top 100 customers by lifetime value
SELECT
    top_customers.customer_id,
    top_customers.lifetime_value,
    c.name,
    c.email
FROM (
    SELECT customer_id, SUM(amount) AS lifetime_value
    FROM orders
    GROUP BY customer_id
    ORDER BY lifetime_value DESC
    LIMIT 100
) AS top_customers
JOIN customers AS c ON top_customers.customer_id = c.customer_id;
```

### Nested derived tables (use CTEs in practice)

```sql
-- Three levels of nesting — readable only as a CTE; shown here for reference
SELECT region, percentile_group
FROM (
    SELECT region, total_revenue,
           NTILE(4) OVER (ORDER BY total_revenue) AS percentile_group
    FROM (
        SELECT region, SUM(amount) AS total_revenue
        FROM orders
        GROUP BY region
    ) AS revenue
) AS quartiles
WHERE percentile_group = 4;
```

### Lateral subquery (LATERAL keyword)

```sql
-- LATERAL allows the derived table to reference columns from the left-side table
SELECT c.customer_id, c.name, latest.last_order_date, latest.last_amount
FROM customers AS c
JOIN LATERAL (
    SELECT order_date AS last_order_date, amount AS last_amount
    FROM orders
    WHERE customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 1
) AS latest ON TRUE;
```

!!! note "LATERAL in Spark SQL"
    `LATERAL` subqueries are supported in Spark SQL 3.x. They allow the inner subquery
    to reference columns from the outer `FROM` clause, enabling correlated inline views.

---

## :material-swap-horizontal: Derived Table vs CTE

| Aspect | Derived Table | CTE (`WITH`) |
|--------|--------------|-------------|
| Syntax location | Inside `FROM (...)` | Before the main `SELECT` |
| Reusable in same query | No — define it again | Yes — reference by name |
| Readability for multi-step logic | Poor (nested) | Excellent (top-down) |
| Nesting | Can be nested | Cannot nest `WITH` blocks |
| Performance | Identical | Identical |
| Best for | Simple one-off inline view | Multi-step pipelines, reused intermediates |

---

## :material-lightbulb-outline: When to Use Derived Tables

| Scenario | Recommendation |
|----------|---------------|
| Filter after aggregation | Derived table in `FROM`, filter in outer `WHERE` |
| Rank then filter top-N | Window function inside derived table, filter outside |
| Join two aggregated results | Two derived tables joined together |
| Simple one-level pre-processing | Derived table (or CTE for readability) |
| Multi-step pipeline | Use CTEs instead |
| Correlated inline view referencing outer table | `LATERAL` subquery |
