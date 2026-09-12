# :material-link-variant: Chained CTEs

Chained CTEs build a top-down pipeline inside one SQL statement. Spark 4.2 resolves them in order, so each step can depend on earlier steps without burying the logic inside nested subqueries.

### :material-animation-play: Interactive Visualization — Resolution Order

<div id="viz-cte-chained-flow" class="ts-viz"></div>

The pipeline highlights how Spark resolves `step1`, then `step2`, then `step3`. It also shows the verified rule that later CTEs can see earlier ones, but not the other way around.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
WITH
step1 AS (
    SELECT ...
    FROM source_table
    WHERE ...
),
step2 AS (
    SELECT ...
    FROM step1
),
step3 AS (
    SELECT ...
    FROM step2
    JOIN step1 ON ...
)
SELECT * FROM step3;
```

Rules:

- CTEs are defined once after a single `WITH` keyword.
- A CTE can reference only CTEs defined earlier in the same list.
- CTE names should describe the data they hold, such as `filtered_orders` or `ranked_customers`.

______________________________________________________________________

## :material-information-outline: Behavior

1. **Ordered resolution** — Spark 4.2 resolves the `WITH` list from top to bottom.
2. **No forward references** — `WITH a AS (SELECT * FROM b), b AS (...)` fails during analysis.
3. **Inner `WITH` blocks are allowed** — a CTE body can contain its own nested `WITH`; Spark 4.2 executed `WITH outer_cte AS (WITH inner_cte AS (...) SELECT ...) SELECT * FROM outer_cte` successfully.
4. **Repeated references are still logical reuse, not guaranteed physical reuse** — if `step1` is referenced in multiple later branches, Spark may expand it multiple times in the plan.
5. **The final statement can be `SELECT` or DML** — a chained `WITH` block can feed `INSERT`, `MERGE`, `UPDATE`, or `DELETE` when the target statement supports it.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Three-step aggregation pipeline

```sql
WITH
current_year_sales AS (
    SELECT
        order_id,
        customer_id,
        region,
        amount,
        order_date
    FROM sales
    WHERE order_date >= DATE '2024-01-01'
      AND order_date < DATE '2025-01-01'
),
customer_totals AS (
    SELECT
        customer_id,
        region,
        SUM(amount) AS total_spent,
        COUNT(*) AS order_count,
        MAX(order_date) AS last_order_date
    FROM current_year_sales
    GROUP BY customer_id, region
),
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
ORDER BY region, region_rank, customer_id;
```

### ETL pipeline: clean, enrich, classify

```sql
WITH
cleaned AS (
    SELECT
        CAST(order_id AS BIGINT) AS order_id,
        TRIM(UPPER(customer_name)) AS customer_name,
        CAST(order_date AS DATE) AS order_date,
        CAST(amount AS DECIMAL(18, 2)) AS amount
    FROM raw_orders
    WHERE order_id IS NOT NULL
      AND amount IS NOT NULL
),
enriched AS (
    SELECT
        c.order_id,
        c.order_date,
        c.amount,
        d.customer_id,
        d.segment,
        d.region
    FROM cleaned AS c
    JOIN dim_customer AS d
        ON c.customer_name = d.customer_name
),
classified AS (
    SELECT
        *,
        CASE
            WHEN amount >= 1000 THEN 'Large'
            WHEN amount >= 200 THEN 'Medium'
            ELSE 'Small'
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

### Funnel analysis across multiple stages

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
    LEFT JOIN signups AS s ON v.user_id = s.user_id
    LEFT JOIN purchases AS p ON v.user_id = p.user_id
)
SELECT
    COUNT(*) AS total_visitors,
    SUM(signed_up) AS total_signups,
    SUM(purchased) AS total_purchasers,
    ROUND(SUM(signed_up) * 100.0 / COUNT(*), 2) AS signup_rate_pct,
    ROUND(SUM(purchased) * 100.0 / COUNT(*), 2) AS purchase_rate_pct
FROM funnel;
```

### Nested `WITH` inside a CTE body

```sql
WITH outer_cte AS (
    WITH inner_cte AS (
        SELECT 1 AS x
    )
    SELECT x + 1 AS y
    FROM inner_cte
)
SELECT *
FROM outer_cte;
```

### Diagnosing a Slow CTE Chain: Recomputation, Not a Bad Plan

Point 4 above ("logical reuse, not guaranteed physical reuse") is the most common
reason a long CTE chain is *correct* but *slow*: if an early CTE is referenced by more
than one later branch, Spark inlines and **re-executes its entire upstream subtree once
per reference** — there's no automatic memoization. Verified on Spark 4.2 with a `base`
CTE consumed by two downstream aggregates:

```sql
WITH
base AS (
    SELECT customer_id, region, amount FROM sales WHERE amount > 0
),
by_customer AS (
    SELECT customer_id, SUM(amount) AS total FROM base GROUP BY customer_id
),
by_region AS (
    SELECT region, SUM(amount) AS total FROM base GROUP BY region
)
SELECT * FROM by_customer c JOIN by_region r ON c.customer_id % 3 = r.region;
```

```sql
EXPLAIN <above query>;
```

```text
+- BroadcastHashJoin ...
   :- HashAggregate(keys=[customer_id], ...)
   :     +- ... Filter (amount > 0.0) ...
   :        +- Range (1, 5001, ...)              <- base's scan+filter, copy #1
   +- HashAggregate(keys=[region], ...)
         +- ... Filter (amount > 0.0) ...
            +- Range (1, 5001, ...)              <- base's scan+filter, copy #2
```

`base`'s `Filter`/`Project` (and the source scan underneath it) shows up **twice** — once
inlined into each branch that reads it. On a real table this means every expensive
upstream transformation (joins, filters, casts) in `base` runs twice, three times, or
once per downstream reference, not once total.

**Fix: materialize the strategic stage that's referenced more than once**, so it
executes exactly one time and every branch reads the materialized result instead of
recomputing it:

```sql
CACHE TABLE base_cached AS
    SELECT customer_id, region, amount FROM sales WHERE amount > 0;

WITH
by_customer AS (
    SELECT customer_id, SUM(amount) AS total FROM base_cached GROUP BY customer_id
),
by_region AS (
    SELECT region, SUM(amount) AS total FROM base_cached GROUP BY region
)
SELECT * FROM by_customer c JOIN by_region r ON c.customer_id % 3 = r.region;
```

```text
+- BroadcastHashJoin ...
   :- HashAggregate(keys=[customer_id], ...)
   :     +- Scan In-memory table base_cached [...]
   :           +- InMemoryRelation [...]           <- computed once
   +- HashAggregate(keys=[region], ...)
         +- Scan In-memory table base_cached [...]
               +- InMemoryRelation [...]           <- reused, not recomputed
```

Both branches now hit `Scan In-memory table base_cached` — the underlying
`Range`/`Filter`/`Project` subtree runs once and its result is reused, instead of once
per downstream reference. The same materialization works with `df.cache()` in
PySpark, or by writing the stage to a real (Delta/Parquet) temp table when the
intermediate result is too large to keep in memory or needs to survive across jobs.

!!! tip "How to spot which stage to materialize"

    Read the `EXPLAIN` output for the whole chain, not just the final CTE. If the same
    subtree (same source table, same filter/project) appears more than once, that CTE
    is being recomputed per reference — it's a strong materialization candidate. Only
    cache/materialize CTEs that are (a) referenced more than once **and** (b)
    expensive enough that recomputation actually costs something; caching every step
    of a chain adds memory pressure and shuffle for no benefit if a CTE is cheap or
    only used once.

______________________________________________________________________

## :material-lightbulb-outline: When to Use Chained CTEs

| Scenario                                                                        | Pattern                                                                                           |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Complex query with several named steps                                          | One CTE per logical transformation                                                                |
| ETL pipeline                                                                    | Clean -> enrich -> classify -> final write                                                        |
| Funnel or cohort analysis                                                       | One CTE per business milestone                                                                    |
| Ranking after aggregation                                                       | Aggregate first, rank second, filter last                                                         |
| Local nested helper logic                                                       | Nested `WITH` inside a CTE body                                                                   |
| Chain runs correctly but is slow, and an early CTE feeds several later branches | Inspect `EXPLAIN` for the duplicated subtree, then `CACHE TABLE` (or `df.cache()`) that one stage |

!!! tip "Keep each CTE narrow"

    Each step should do one main transformation. Small, well-named CTEs are easier to test,
    explain, and move into temp views later if reuse becomes more important than locality.
