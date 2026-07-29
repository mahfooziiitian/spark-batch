# :material-numeric-1-circle: De-duplication

Remove duplicate rows by keeping the most recent sale per rep using `ROW_NUMBER`.

---

## :material-flask-outline: Practical Examples

=== "Standard"

    ```sql
    SELECT region, rep, sale_date, amount
    FROM (
        SELECT
            region, rep, sale_date, amount,
            ROW_NUMBER() OVER (PARTITION BY rep ORDER BY sale_date DESC) AS rn
        FROM sales
    )
    WHERE rn = 1;
    ```

=== "QUALIFY (Spark 3.3+)"

    ```sql
    SELECT region, rep, sale_date, amount
    FROM sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rep ORDER BY sale_date DESC) = 1;
    ```

=== "Pipe Syntax (Spark 4.0)"

    ```sql
    FROM sales
    |> SELECT *, ROW_NUMBER() OVER (PARTITION BY rep ORDER BY sale_date DESC) AS rn
    |> WHERE rn = 1
    |> SELECT region, rep, sale_date, amount;
    ```

```
-- Result:
-- | region | rep   | sale_date  | amount |
-- |--------|-------|------------|--------|
-- | North  | Alice | 2024-01-10 |    300 |
-- | North  | Bob   | 2024-01-06 |    300 |
-- | South  | Carol | 2024-01-07 |    500 |
```

---

## :material-lightbulb-outline: When to Use

- Keep the **latest record** per entity (e.g., most recent address per customer).
- SCD Type 1 — retain only the current version of each row.
- Any scenario where a unique key should map to exactly one row.

!!! tip "Performance"
    For very large tables, add a date filter before the window to reduce the partition size.
    `ROW_NUMBER` is cheaper than `RANK` when you only need one row per key.

---

## :material-arrow-right: Related

- [Top-N Per Group](top_n.md) — extend this pattern to keep N rows per key
- [Window Types — Ranking](../functions/ranking.md) — `ROW_NUMBER`, `RANK`, `DENSE_RANK`
