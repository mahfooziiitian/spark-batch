# :material-link-chain: Chained CTEs

Chained CTEs build a multi-step pipeline inside a single SQL statement. Each CTE
represents one transformation layer; later CTEs can reference all earlier ones. The
result is a readable, top-down data flow with no nested subqueries.

---

## :material-code-tags: Syntax

```sql
WITH
step1 AS (
    -- first transformation
    SELECT ...
    FROM   source_table
    WHERE  ...
),
step2 AS (
    -- build on step1
    SELECT ...
    FROM   step1
),
step3 AS (
    -- build on step1 and/or step2
    SELECT ...
    FROM   step2
    JOIN   step1 ON ...
)
SELECT * FROM step3;
```

Rules:
- CTEs are defined in order; a CTE can only reference CTEs **above** it in the list.
- All CTEs share the same `WITH` keyword — do not repeat `WITH` for each step.
- CTE names use `snake_case` and describe the logical role (`filtered_orders`, `ranked_customers`).

---

## :material-information-outline: Behavior

1. Spark's Catalyst optimizer **inlines** each CTE at its reference sites by default — there is no automatic materialization boundary between steps.
2. Because CTEs are inlined, a CTE referenced **N times** may be evaluated **N times**. Wrap a reused CTE in a temp view and optionally cache it when this matters for performance.
3. Column aliases defined inside a CTE are visible only within that CTE and to CTEs or the final `SELECT` that reference it.
4. The final `SELECT` (or DML statement) **must** appear after the last CTE in the `WITH` block.

---

## :material-flask-outline: Practical Examples

### Three-step aggregation pipeline

```sql
WITH
-- Step 1: restrict to the current year
current_year_sales AS (
    SELECT
        order_id,
        customer_id,
        region,
        amount,
        order_date
    FROM sales
    WHERE order_date >= '2024-01-01'
      AND order_date <  '2025-01-01'
),
-- Step 2: aggregate per customer
customer_totals AS (
    SELECT
        customer_id,
        region,
        SUM(amount)   AS total_spent,
        COUNT(*)      AS order_count,
        MAX(order_date) AS last_order_date
    FROM current_year_sales
    GROUP BY customer_id, region
),
-- Step 3: rank within each region
ranked AS (
    SELECT
        customer_id,
        region,
        total_spent,
        order_count,
        last_order_date,
        RANK() OVER (PARTITION BY region ORDER BY total_spent DESC) AS region_rank
    FROM customer_totals
)
SELECT *
FROM ranked
WHERE region_rank <= 5
ORDER BY region, region_rank;
```

### ETL: clean → enrich → load

```sql
WITH
-- Step 1: remove nulls and trim strings
cleaned AS (
    SELECT
        CAST(order_id AS BIGINT)        AS order_id,
        TRIM(UPPER(customer_name))      AS customer_name,
        CAST(order_date AS DATE)        AS order_date,
        CAST(amount AS DECIMAL(18, 2))  AS amount
    FROM raw_orders
    WHERE order_id IS NOT NULL
      AND amount   IS NOT NULL
),
-- Step 2: join with the customer dimension for enrichment
enriched AS (
    SELECT
        c_orders.order_id,
        c_orders.order_date,
        c_orders.amount,
        dim.customer_id,
        dim.segment,
        dim.region
    FROM cleaned AS c_orders
    JOIN dim_customer AS dim
        ON c_orders.customer_name = dim.customer_name
),
-- Step 3: classify order size
classified AS (
    SELECT
        *,
        CASE
            WHEN amount >= 1000 THEN 'Large'
            WHEN amount >= 200  THEN 'Medium'
            ELSE                     'Small'
        END AS order_size
    FROM enriched
)
INSERT INTO fact_orders
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    segment,
    region,
    order_size
FROM classified;
```

### Funnel analysis across multiple steps

```sql
WITH
visits AS (
    SELECT user_id, MIN(event_time) AS first_visit
    FROM events
    WHERE event_type = 'page_view'
    GROUP BY user_id
),
signups AS (
    SELECT user_id, MIN(event_time) AS signup_time
    FROM events
    WHERE event_type = 'signup'
    GROUP BY user_id
),
purchases AS (
    SELECT user_id, MIN(event_time) AS first_purchase
    FROM events
    WHERE event_type = 'purchase'
    GROUP BY user_id
),
funnel AS (
    SELECT
        v.user_id,
        v.first_visit,
        s.signup_time,
        p.first_purchase,
        CASE WHEN s.user_id IS NOT NULL THEN 1 ELSE 0 END AS signed_up,
        CASE WHEN p.user_id IS NOT NULL THEN 1 ELSE 0 END AS purchased
    FROM visits AS v
    LEFT JOIN signups   AS s ON v.user_id = s.user_id
    LEFT JOIN purchases AS p ON v.user_id = p.user_id
)
SELECT
    COUNT(*)                          AS total_visitors,
    SUM(signed_up)                    AS total_signups,
    SUM(purchased)                    AS total_purchasers,
    ROUND(SUM(signed_up)    * 100.0 / COUNT(*), 2) AS signup_rate_pct,
    ROUND(SUM(purchased)    * 100.0 / COUNT(*), 2) AS purchase_rate_pct
FROM funnel;
```

### Reference an earlier CTE twice

```sql
WITH
order_totals AS (
    SELECT customer_id, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer_id
),
stats AS (
    SELECT AVG(total_spent) AS avg_spend, STDDEV(total_spent) AS stddev_spend
    FROM order_totals
)
-- order_totals referenced again here alongside stats
SELECT
    ot.customer_id,
    ot.total_spent,
    ROUND((ot.total_spent - st.avg_spend) / st.stddev_spend, 2) AS z_score
FROM order_totals AS ot
CROSS JOIN stats AS st
ORDER BY z_score DESC;
```

---

## :material-lightbulb-outline: When to Use Chained CTEs

| Scenario | Pattern |
|----------|---------|
| Complex query with 3+ logical steps | One CTE per step, named by role |
| ETL pipeline: clean → enrich → classify | Sequential chained CTEs + final `INSERT` |
| Funnel / cohort analysis | One CTE per funnel stage, final `LEFT JOIN` chain |
| Ranking after aggregation | Aggregate in CTE 1, rank in CTE 2, filter in final `SELECT` |
| Intermediate result reused in two places | Extract to a CTE; cache if expensive |

!!! tip "Name CTEs by what they represent, not how they work"
    `filtered_orders` is better than `step1`. Future readers understand the role
    immediately without tracing back through the logic.

!!! note "Materialization"
    If a CTE is expensive (e.g., a large aggregation) and referenced more than once,
    wrap it in `CREATE OR REPLACE TEMP VIEW ... AS (...)` and optionally `CACHE TABLE`
    to avoid re-computation.
