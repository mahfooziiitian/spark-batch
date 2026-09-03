-- Aggregate window functions: running totals, partition totals, moving averages,
-- rolling max, cumulative contribution %, CUME_DIST, and partition size via COUNT(*).

CREATE OR REPLACE TEMP VIEW sales AS
SELECT -- noqa: LT09
    * FROM VALUES
('North', 'Alice', DATE '2024-01-01', 100),
('North', 'Alice', DATE '2024-01-05', 200),
('North', 'Alice', DATE '2024-01-10', 300),
('North', 'Bob', DATE '2024-01-02', 150),
('North', 'Bob', DATE '2024-01-06', 250),
('South', 'Alice', DATE '2024-01-03', 400),
('South', 'Alice', DATE '2024-01-07', 500),
('South', 'Bob', DATE '2024-01-04', 180),
('South', 'Bob', DATE '2024-01-08', 220)
    AS sales (region, rep, sale_date, amount);

---
--- 1. Running total — cumulative SUM ordered by sale_date within each (region, rep)
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales
ORDER BY region, rep, sale_date;
-- Result: running_total increases with each sale; resets for each (region, rep) pair.

---
--- 2. Partition total — SUM across the entire partition (no ORDER BY, no frame)
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (PARTITION BY region) AS region_total
FROM sales
ORDER BY region, rep, sale_date;
-- Result: region_total is the same constant value on every row within the same region.

---
--- 3. Moving average — 3-row centred window (1 PRECEDING to 1 FOLLOWING)
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    ROUND(
        AVG(amount) OVER (
            PARTITION BY region, rep
            ORDER BY sale_date
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ),
        2
    ) AS moving_avg_3
FROM sales
ORDER BY region, rep, sale_date;
-- Result: boundary rows average fewer than 3 values (the frame clips at partition edges).

---
--- 4. Rolling 3-row maximum
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    MAX(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_max_3
FROM sales
ORDER BY region, rep, sale_date;
-- Result: rolling_max_3 reflects the highest amount seen in the current and previous 2 rows.

---
--- 5. Cumulative % contribution — running_sum / partition_total
---

WITH totals AS (
    SELECT
        region,
        rep,
        sale_date,
        amount,
        SUM(amount) OVER (
            PARTITION BY region, rep
            ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_sum,
        SUM(amount) OVER (PARTITION BY region, rep) AS partition_total
    FROM sales
)

SELECT
    region,
    rep,
    sale_date,
    amount,
    running_sum,
    ROUND(running_sum / partition_total * 100, 1) AS cumulative_pct
FROM totals
ORDER BY region, rep, sale_date;
-- Result: last row per group always shows 100.0 %.

---
--- 6. CUME_DIST — fraction of partition rows with amount <= current row's amount
---

SELECT
    region,
    rep,
    amount,
    ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY amount), 4)
        AS cume_distribution
FROM sales
ORDER BY region, amount;
-- Result: cume_distribution reaches 1.0 on the highest-amount row within each region.

---
--- 7. Partition size — COUNT(*) OVER to attach total row count per region
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    COUNT(*) OVER (PARTITION BY region) AS region_row_count
FROM sales
ORDER BY region, rep, sale_date;
-- Result: region_row_count = 5 for North and 4 for South on every row in that region.
