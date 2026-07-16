# :material-numeric-3-circle: Running Balance

Compute a cumulative sales total per rep ordered chronologically.

---

## :material-flask-outline: Practical Examples

```sql
SELECT
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_balance
FROM sales
ORDER BY rep, sale_date;
-- Result:
-- | rep   | sale_date  | amount | running_balance |
-- |-------|------------|--------|-----------------|
-- | Alice | 2024-01-01 |    100 |             100 |
-- | Alice | 2024-01-05 |    200 |             300 |
-- | Alice | 2024-01-10 |    300 |             600 |
-- | Bob   | 2024-01-02 |    150 |             150 |
-- | Bob   | 2024-01-06 |    300 |             450 |
-- | Carol | 2024-01-03 |    400 |             400 |
-- | Carol | 2024-01-07 |    500 |             900 |
```

---

## :material-lightbulb-outline: When to Use

- Financial ledgers — running account balance after each transaction.
- Inventory tracking — cumulative stock in / stock out over time.
- Progress monitoring — cumulative completion percentage.

!!! tip "Frame matters"
    `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is explicit and recommended.
    Without it, the default `RANGE` frame can produce unexpected results when
    `ORDER BY` values are not unique.

---

## :material-arrow-right: Related

- [Frame — ROWS](../frame/rows.md) — how `ROWS BETWEEN` boundaries work
- [Period-over-Period](period_comparison.md) — compare each row to the previous one
