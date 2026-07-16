# :material-numeric-10-circle: Year-over-Year (YoY) Comparison

Compare each month's revenue to the same month in the prior year.

---

## :material-flask-outline: Practical Examples

```sql
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date)  AS month,
        region,
        SUM(amount)                      AS revenue
    FROM orders
    GROUP BY 1, 2
)
SELECT
    month,
    region,
    revenue,
    LAG(revenue, 12) OVER (
        PARTITION BY region
        ORDER BY month
    )                                                          AS revenue_prior_year,
    ROUND(
        100.0 * (revenue - LAG(revenue, 12) OVER (PARTITION BY region ORDER BY month))
              / NULLIF(LAG(revenue, 12) OVER (PARTITION BY region ORDER BY month), 0),
        1
    )                                                          AS yoy_pct
FROM monthly
ORDER BY region, month;
```

---

## :material-information-outline: How It Works

1. **CTE** aggregates raw orders into monthly revenue per region.
2. **`LAG(revenue, 12)`** looks back 12 rows (months) to fetch the same month's revenue from last year.
3. **`NULLIF`** prevents division by zero when the prior year has no data.

!!! warning "LAG offset assumes contiguous months"
    `LAG(col, 12)` works correctly only when every month has a row.
    If months can be missing, use a `JOIN` on the calendar month instead.

---

## :material-lightbulb-outline: When to Use

- Executive dashboards — YoY growth by region or product line.
- Seasonal businesses — compare same-season performance.
- Trend analysis — detect acceleration or deceleration over years.

---

## :material-arrow-right: Related

- [Period-over-Period](period_comparison.md) — MoM / WoW comparison with `LAG(col, 1)`
- [Window Types — Navigation](../window/navigation.md) — `LAG`, `LEAD` reference
