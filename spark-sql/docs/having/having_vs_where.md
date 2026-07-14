# :material-compare: WHERE vs HAVING

Both clauses filter data, but they operate at different stages of the query pipeline and have very different performance implications.

---

## :material-sitemap: Execution Order

```mermaid
flowchart LR
    FROM --> WHERE["WHERE\n(row filter)"]
    WHERE --> GROUP["GROUP BY\n(aggregation)"]
    GROUP --> HAVING["HAVING\n(group filter)"]
    HAVING --> SELECT["SELECT / ORDER BY"]
```

`WHERE` runs **before** grouping — it reduces the number of rows that enter the aggregation.
`HAVING` runs **after** grouping — it removes entire groups from the result.

---

## :material-table: Side-by-Side Comparison

| Factor | WHERE | HAVING |
|--------|-------|--------|
| When it runs | Before `GROUP BY` | After `GROUP BY` |
| What it filters | Individual rows | Aggregated groups |
| Can reference raw columns | Yes | Yes (only if in `GROUP BY`) |
| Can reference aggregates | No | Yes |
| Performance | Better — reduces shuffle input | Worse — discards groups after work is done |
| Required for | Row predicates | Aggregate predicates |

---

## :material-flask-outline: Examples

### Example 1 — Correct placement

```sql
-- Wrong: pushes a row predicate to HAVING — wastes aggregation work
SELECT region, SUM(amount) AS total
FROM orders
GROUP BY region
HAVING region = 'APAC';

-- Correct: row predicate belongs in WHERE
SELECT region, SUM(amount) AS total
FROM orders
WHERE region = 'APAC'
GROUP BY region;
```

### Example 2 — Both clauses together

```sql
-- WHERE trims rows before grouping; HAVING trims groups after
SELECT
    product_category,
    SUM(amount)     AS total_revenue,
    COUNT(*)        AS order_count
FROM orders
WHERE order_date >= '2024-01-01'       -- row filter: skip old orders
GROUP BY product_category
HAVING SUM(amount) > 50000             -- group filter: keep high-revenue categories
ORDER BY total_revenue DESC;
```

### Example 3 — Aggregate filter requires HAVING

```sql
-- Cannot use WHERE for aggregate conditions
-- Wrong: parse error
SELECT customer_id, COUNT(*) AS cnt
FROM orders
WHERE COUNT(*) > 5              -- ERROR: aggregate not allowed in WHERE
GROUP BY customer_id;

-- Correct
SELECT customer_id, COUNT(*) AS cnt
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5;
```

### Example 4 — Date range in WHERE, aggregate threshold in HAVING

```sql
SELECT
    salesperson_id,
    SUM(deal_value)    AS total_deals,
    AVG(deal_value)    AS avg_deal
FROM deals
WHERE close_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY salesperson_id
HAVING SUM(deal_value) > 100000
   AND AVG(deal_value) > 5000;
```

---

## :material-speedometer: Performance Impact

| Scenario | Recommendation |
|----------|---------------|
| Filter by a raw column | Always use `WHERE` |
| Filter by an aggregate value | Must use `HAVING` |
| Both types of filter in one query | Put row filters in `WHERE`, aggregate filters in `HAVING` |
| Very selective row predicate | Put in `WHERE` — reduces data sent to shuffle |

!!! tip
    Moving a filter from `HAVING` to `WHERE` (when valid) can dramatically reduce
    the amount of data shuffled across the network during `GROUP BY`.

---

## :material-alert-circle: Common Mistakes

| Mistake | Fix |
|---------|-----|
| `HAVING col = 'value'` instead of `WHERE col = 'value'` | Move to `WHERE` |
| `WHERE SUM(col) > n` | Change to `HAVING SUM(col) > n` |
| `HAVING COUNT(*) > 0` without any filtering intent | Harmless but unnecessary |
| Division in `HAVING` without `NULLIF` | Wrap denominator: `NULLIF(SUM(x), 0)` |
