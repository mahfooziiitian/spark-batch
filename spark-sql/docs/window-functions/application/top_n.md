# :material-numeric-2-circle: Top-N Per Group

Return the top 2 sales per region, ranked by amount descending.

---

## :material-flask-outline: Practical Examples

=== "Standard"

    ```sql
    SELECT region, rep, sale_date, amount
    FROM (
        SELECT
            region, rep, sale_date, amount,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
        FROM sales
    )
    WHERE rn <= 2;
    ```

=== "QUALIFY"

    ```sql
    SELECT region, rep, sale_date, amount
    FROM sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) <= 2;
    ```

```
-- Result:
-- | region | rep   | sale_date  | amount |
-- |--------|-------|------------|--------|
-- | North  | Alice | 2024-01-10 |    300 |
-- | North  | Bob   | 2024-01-06 |    300 |
-- | South  | Carol | 2024-01-07 |    500 |
-- | South  | Carol | 2024-01-03 |    400 |
```

---

## :material-information-outline: ROW_NUMBER vs RANK vs DENSE_RANK

!!! note "Choosing the right ranking function"
    - `ROW_NUMBER` — exactly N rows, ties broken arbitrarily
    - `RANK` — may return more than N rows if there are ties
    - `DENSE_RANK` — no gaps in ranking, useful for "top N distinct values"

---

## :material-lightbulb-outline: When to Use

- Leaderboards and dashboards showing the top performers per category.
- Report slicing — "top 5 products per region by revenue".
- Sampling — pick N representative rows per group.

---

## :material-arrow-right: Related

- [De-duplication](deduplication.md) — special case where N = 1
- [Window Types — Ranking](../window/ranking.md) — full ranking function reference
