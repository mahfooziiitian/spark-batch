# :material-numeric-4-circle: Period-over-Period Comparison

Compute the sale-over-sale delta percentage relative to the previous sale for each rep.

---

## :material-flask-outline: Practical Examples

```sql
SELECT
    rep,
    sale_date,
    amount,
    LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_amount,
    ROUND(
        100.0 * (amount - LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date))
              / LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date),
        1
    ) AS delta_pct
FROM sales
ORDER BY rep, sale_date;
-- Result:
-- | rep   | sale_date  | amount | prev_amount | delta_pct |
-- |-------|------------|--------|-------------|-----------|
-- | Alice | 2024-01-01 |    100 |        NULL |      NULL |
-- | Alice | 2024-01-05 |    200 |         100 |     100.0 |
-- | Alice | 2024-01-10 |    300 |         200 |      50.0 |
-- | Bob   | 2024-01-02 |    150 |        NULL |      NULL |
-- | Bob   | 2024-01-06 |    300 |         150 |     100.0 |
-- | Carol | 2024-01-03 |    400 |        NULL |      NULL |
-- | Carol | 2024-01-07 |    500 |         400 |      25.0 |
```

---

## :material-lightbulb-outline: When to Use

- Month-over-month or week-over-week growth metrics.
- Detecting sudden spikes or drops in a time series.
- Sales performance dashboards comparing current to previous period.

!!! tip "Avoid repeated LAG calls"
    The query above calls `LAG(amount)` three times with the same `OVER` spec.
    Use a CTE or named `WINDOW` to compute it once and reuse the result.

---

## :material-arrow-right: Related

- [YoY Comparison](yoy_comparison.md) — same concept across years with `LAG(col, 12)`
- [Window Types — Navigation](../window/navigation.md) — `LAG`, `LEAD`, `FIRST_VALUE`
