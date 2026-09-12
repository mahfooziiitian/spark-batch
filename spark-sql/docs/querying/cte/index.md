# :material-recycle: Common Table Expressions (CTEs)

A CTE (`WITH` clause) defines a named result set scoped to one SQL statement. In Spark SQL 4.2, CTEs are excellent for readable pipelines, reusable intermediate logic, and recursive queries with `WITH RECURSIVE`.

### :material-animation-play: Interactive Visualization — Statement Scope and Reuse

<div id="viz-cte-overview" class="ts-viz"></div>

This demo contrasts a single-reference CTE with a multi-reference CTE. It mirrors the verified Spark 4.2 behavior: scope is statement-local, and repeated references should not be assumed to compute only once.

______________________________________________________________________

## :material-sitemap: In This Section

| Page                                      | Covers                                                    |
| ----------------------------------------- | --------------------------------------------------------- |
| [Chained CTEs](chained.md)                | Multi-step pipelines with sequential CTE dependencies     |
| [CTE in DML](cte-in-dml.md)               | Using CTEs with `INSERT`, `MERGE`, `UPDATE`, and `DELETE` |
| [Recursive CTE](recursive.md)             | `WITH RECURSIVE` for sequences and hierarchies            |
| [CTE vs Temp View](cte-vs-temp-view.md)   | Scope, caching, and plan reuse                            |
| [CTE for Deduplication](deduplication.md) | `ROW_NUMBER()` and `QUALIFY` patterns                     |
| [CTE for Pivoting](pivot.md)              | `PIVOT`, manual pivot, and `UNPIVOT`                      |

______________________________________________________________________

## :material-code-tags: Syntax

**Single CTE:**

```sql
WITH cte_name AS (
    SELECT ...
    FROM ...
    WHERE ...
)
SELECT * FROM cte_name;
```

**Multiple chained CTEs:**

```sql
WITH cte1 AS (
    SELECT ...
),
cte2 AS (
    SELECT ...
    FROM cte1
    WHERE ...
)
SELECT * FROM cte2;
```

**CTE before DML:**

```sql
WITH prepared AS (
    SELECT ...
)
INSERT INTO target_table
SELECT * FROM prepared;
```

**Recursive CTE:**

```sql
WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM numbers
    WHERE n < 5
)
SELECT * FROM numbers;
```

______________________________________________________________________

## :material-information-outline: Behavior

1. **Statement scope only** — a CTE is visible only inside the statement that defines it.
2. **Definition order matters** — later CTEs can reference earlier ones, but forward references fail. In Spark 4.2, `WITH a AS (SELECT * FROM b), b AS (...)` raises `TABLE_OR_VIEW_NOT_FOUND`.
3. **No guaranteed materialization** — in a verified Spark 4.2 self-join example, `EXPLAIN FORMATTED` expanded the same CTE into two aggregate branches and did not show `ReusedExchange`. Treat repeated references as potentially re-evaluated.
4. **Nested `WITH` blocks are valid** — a CTE body can contain its own inner `WITH` block when that improves readability.
5. **Recursive CTEs work in Spark 4.2** — `WITH RECURSIVE` runs even with `spark.sql.ansi.enabled = false`.

______________________________________________________________________

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
    (1, 'Alice',   DATE '2024-01-15', 250.00),
    (2, 'Bob',     DATE '2024-01-16', 120.00),
    (3, 'Alice',   DATE '2024-01-17', 300.00),
    (4, 'Charlie', DATE '2024-01-18',  80.00),
    (5, 'Bob',     DATE '2024-01-19', 450.00),
    (6, 'Alice',   DATE '2024-01-20', 175.00)
AS orders(order_id, customer, order_date, amount);
```

### Single CTE: filter, then aggregate

```sql
WITH recent_orders AS (
    SELECT order_id, customer, amount
    FROM orders
    WHERE order_date >= DATE '2024-01-17'
)
SELECT
    customer,
    SUM(amount) AS total_amount,
    COUNT(*) AS order_count
FROM recent_orders
GROUP BY customer
ORDER BY total_amount DESC;
```

### Chained CTEs: step-by-step pipeline

```sql
WITH order_totals AS (
    SELECT
        customer,
        SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
),
ranked_customers AS (
    SELECT
        customer,
        total_spent,
        RANK() OVER (ORDER BY total_spent DESC) AS spend_rank
    FROM order_totals
)
SELECT customer, total_spent, spend_rank
FROM ranked_customers
WHERE spend_rank <= 2
ORDER BY spend_rank, customer;
```

### Reused CTE reference inside one statement

```sql
WITH customer_totals AS (
    SELECT
        customer,
        SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
)
SELECT
    ct.customer,
    ct.total_spent,
    ROUND(avg_all.avg_spend, 2) AS overall_avg,
    ROUND(ct.total_spent - avg_all.avg_spend, 2) AS diff_from_avg
FROM customer_totals AS ct
CROSS JOIN (
    SELECT AVG(total_spent) AS avg_spend
    FROM customer_totals
) AS avg_all
ORDER BY ct.customer;
```

### CTE before `INSERT`

```sql
WITH prepared AS (
    SELECT customer, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
)
INSERT INTO customer_summary
SELECT * FROM prepared;
```

### Recursive CTE: integer sequence

```sql
WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM numbers
    WHERE n < 5
)
SELECT n
FROM numbers
ORDER BY n;
```

______________________________________________________________________

## :material-swap-horizontal: CTE vs Subquery

| Aspect                 | CTE                        | Subquery                   |
| ---------------------- | -------------------------- | -------------------------- |
| Readability            | Named once at the top      | Embedded inline            |
| Reuse in one statement | Yes                        | Usually duplicated         |
| Recursion              | Yes, with `WITH RECURSIVE` | No                         |
| Scope                  | One statement              | One expression/query block |
| Materialization        | Not guaranteed             | Not guaranteed             |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                               | Recommended Pattern          |
| ------------------------------------------------------ | ---------------------------- |
| Breaking a query into named steps                      | Chained CTEs                 |
| Reusing an intermediate once or twice in one statement | CTE                          |
| Reusing the same logic across multiple statements      | Temp view, optionally cached |
| Generating sequences or traversing hierarchies         | Recursive CTE                |
| Preparing rows before `INSERT`                         | CTE + DML                    |
