# :material-numeric-9-circle: Median and Approximate Percentiles

Compute median and arbitrary percentiles within each partition.

---

## :material-flask-outline: Practical Examples

### Approximate percentiles (recommended for large datasets)

```sql
SELECT DISTINCT
    region,
    PERCENTILE_APPROX(amount, 0.5)          AS median_amount,
    PERCENTILE_APPROX(amount, 0.25)         AS p25,
    PERCENTILE_APPROX(amount, 0.75)         AS p75,
    PERCENTILE_APPROX(amount, array(0.05, 0.95))
                                             AS p5_p95
FROM sales
GROUP BY region;
```

### Exact percentile (small datasets only)

```sql
-- Triggers a full sort — avoid on large partitions
SELECT DISTINCT
    region,
    PERCENTILE(amount, 0.5) AS exact_median
FROM sales
GROUP BY region;
```

---

## :material-information-outline: Window vs GROUP BY

!!! tip "Window vs GROUP BY for percentiles"
    Percentile functions are aggregate functions, not window functions in Spark SQL.
    Use them with `GROUP BY` (or `OVER` with a full-partition frame and `DISTINCT`) for
    per-group percentiles. For row-level percentile rank, use `PERCENT_RANK()` or
    `CUME_DIST()` as window functions.

---

## :material-lightbulb-outline: When to Use

- Summary statistics dashboards — median salary, revenue, latency per group.
- Outlier detection — flag values outside the p5–p95 range.
- Data profiling — understand distribution shape across partitions.

---

## :material-arrow-right: Related

- [Percentile Scoring](percentile.md) — row-level `PERCENT_RANK` and `NTILE`
- [Window Types — Aggregate](../window/aggregate.md) — `SUM`, `AVG`, `COUNT` as windows
