# :material-numeric-8-circle: Gap Detection

Flag rows where the gap to the next event exceeds a threshold,
then group consecutive events into "streaks".

---

## :material-flask-outline: Practical Examples

```sql
SELECT
    rep,
    sale_date,
    amount,
    days_to_next,
    -- A new streak starts whenever there is a gap > 5 days (or it is the first row)
    SUM(
        CASE WHEN days_to_next > 5 OR days_to_next IS NULL THEN 1 ELSE 0 END
    ) OVER (
        PARTITION BY rep
        ORDER BY sale_date DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS streak_id
FROM (
    SELECT
        rep,
        sale_date,
        amount,
        DATEDIFF(
            LEAD(sale_date) OVER (PARTITION BY rep ORDER BY sale_date),
            sale_date
        ) AS days_to_next
    FROM sales
)
ORDER BY rep, sale_date;
```

---

## :material-information-outline: How It Works

1. **`LEAD`** looks ahead to the next row's date within each partition.
2. **`DATEDIFF`** measures the gap. `NULL` means no next row (end of partition).
3. **Cumulative `SUM`** over the gap flag (scanning in reverse) assigns streak ids.

---

## :material-lightbulb-outline: When to Use

- Detect missing data in a time series (e.g., days with no transactions).
- Identify streaks of consecutive activity (login streaks, winning streaks).
- Data quality monitoring — flag unexpected gaps in scheduled jobs.

---

## :material-arrow-right: Related

- [Sessionisation](sessionisation.md) — group events into sessions based on gap thresholds
- [Forward-Fill](forward_fill.md) — fill gaps instead of detecting them
