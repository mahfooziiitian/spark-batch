# :material-numeric-2-circle: Top-N Per Group

Return the top 2 sales per region, ranked by amount descending.

______________________________________________________________________

## :material-alert-outline: Common Pitfall — A Global `LIMIT` Is Not "Top-N Per Group"

```sql
-- BUG: this is the top 2 rows OVERALL, not the top 2 per region.
SELECT region, rep, amount
FROM sales
ORDER BY amount DESC
LIMIT 2;
```

```text
-- region  rep    amount
-- South   Carol   500
-- South   Carol   400
```

A single global `ORDER BY ... LIMIT N` sorts the **entire** result set and keeps only
the first `N` rows, with no awareness of `region` at all. If one region happens to have
the largest deals, it can crowd out every other region's top rows entirely — here,
`North`'s top sales (300, 300) never appear because South's two largest deals outrank
them globally. `LIMIT` caps the whole query's output, not each group's.

The fix is a window function: rank rows **within each partition** with `ROW_NUMBER()`
(or `RANK()`/`DENSE_RANK()` — see below), then filter to the top N per partition — this
is exactly the pattern in the examples below, and it correctly returns both regions'
own top performers.

______________________________________________________________________

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

______________________________________________________________________

## :material-information-outline: ROW_NUMBER vs RANK vs DENSE_RANK

!!! note "Choosing the right ranking function"

    - `ROW_NUMBER` — exactly N rows, ties broken arbitrarily
    - `RANK` — may return more than N rows if there are ties
    - `DENSE_RANK` — no gaps in ranking, useful for "top N distinct values"

______________________________________________________________________

## :material-lightbulb-outline: When to Use

- Leaderboards and dashboards showing the top performers per category.
- Report slicing — "top 5 products per region by revenue".
- Sampling — pick N representative rows per group.

______________________________________________________________________

## :material-arrow-right: Related

- [De-duplication](deduplication.md) — special case where N = 1
- [Window Types — Ranking](../functions/ranking.md) — full ranking function reference
