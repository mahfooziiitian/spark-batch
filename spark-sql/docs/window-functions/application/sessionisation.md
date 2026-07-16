# :material-numeric-5-circle: Sessionisation

Detect gaps of more than 3 days between consecutive sales for the same rep
and assign a session id by accumulating gap flags.

---

## :material-flask-outline: Practical Examples

```sql
SELECT
    rep,
    sale_date,
    gap_flag,
    SUM(gap_flag) OVER (PARTITION BY rep ORDER BY sale_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM (
    SELECT
        rep,
        sale_date,
        CASE
            WHEN DATEDIFF(
                sale_date,
                LAG(sale_date) OVER (PARTITION BY rep ORDER BY sale_date)
            ) > 3 THEN 1
            ELSE 0
        END AS gap_flag
    FROM sales
)
ORDER BY rep, sale_date;
-- Result:
-- | rep   | sale_date  | gap_flag | session_id |
-- |-------|------------|----------|------------|
-- | Alice | 2024-01-01 |        0 |          0 |
-- | Alice | 2024-01-05 |        1 |          1 |  -- 4-day gap → new session
-- | Alice | 2024-01-10 |        1 |          2 |  -- 5-day gap → new session
-- | Bob   | 2024-01-02 |        0 |          0 |
-- | Bob   | 2024-01-06 |        1 |          1 |  -- 4-day gap → new session
-- | Carol | 2024-01-03 |        0 |          0 |
-- | Carol | 2024-01-07 |        1 |          1 |  -- 4-day gap → new session
```

---

## :material-information-outline: How It Works

1. **`LAG`** fetches the previous row's date within each rep partition.
2. **`DATEDIFF`** measures the gap in days. If > threshold → `gap_flag = 1`.
3. **Cumulative `SUM`** of the gap flag creates an incrementing session id.

---

## :material-lightbulb-outline: When to Use

- Clickstream analysis — group page views into user sessions.
- IoT event processing — cluster sensor readings into activity bursts.
- User engagement — identify active vs inactive periods.

---

## :material-arrow-right: Related

- [Gap Detection](gap_detection.md) — find and flag gaps without grouping into sessions
- [Running Balance](running_balance.md) — same cumulative `SUM` technique
