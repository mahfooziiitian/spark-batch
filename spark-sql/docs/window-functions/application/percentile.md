# :material-numeric-6-circle: Percentile Scoring

Assign each rep a percentile rank and quartile bucket across their region
using `PERCENT_RANK` and `NTILE(4)`.

---

## :material-flask-outline: Practical Examples

```sql
SELECT
    region,
    rep,
    amount,
    ROUND(PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount), 2) AS pct_rank,
    NTILE(4)             OVER (PARTITION BY region ORDER BY amount)     AS quartile
FROM sales
ORDER BY region, amount;
-- Result:
-- | region | rep   | amount | pct_rank | quartile |
-- |--------|-------|--------|----------|----------|
-- | North  | Alice |    100 |     0.00 |        1 |
-- | North  | Bob   |    150 |     0.25 |        1 |
-- | North  | Alice |    200 |     0.50 |        2 |
-- | North  | Alice |    300 |     0.75 |        3 |
-- | North  | Bob   |    300 |     0.75 |        4 |
-- | South  | Carol |    400 |     0.00 |        1 |
-- | South  | Carol |    500 |     1.00 |        4 |
```

---

## :material-lightbulb-outline: When to Use

- Assign customers to tier buckets (quartiles, deciles) for segmentation.
- Benchmark individual performance against the group distribution.
- Identify outliers — rows in the top or bottom percentile.

!!! note "PERCENT_RANK vs CUME_DIST"
    `PERCENT_RANK` = (rank - 1) / (partition_size - 1), always starts at 0.
    `CUME_DIST` = rank / partition_size, always ends at 1.

---

## :material-arrow-right: Related

- [Median and Percentiles](median.md) — `PERCENTILE_APPROX` for group-level stats
- [Window Types — Ranking](../window/ranking.md) — `PERCENT_RANK`, `NTILE`, `CUME_DIST`
