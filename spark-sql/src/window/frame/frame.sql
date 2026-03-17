-- Window frame specification: ROWS vs RANGE, sliding windows, date-based RANGE,
-- suffix sums, and a direct ROWS vs RANGE comparison showing tie behaviour.

CREATE OR REPLACE TEMP VIEW sales AS
SELECT
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
--- 1. ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW — classic running total
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
-- Result: cumulative sum grows row-by-row; each new sale is added to the prior total.

---
--- 2. ROWS BETWEEN 2 PRECEDING AND CURRENT ROW — sliding 3-row window
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS sliding_3row_sum
FROM sales
ORDER BY region, rep, sale_date;
-- Result: sum covers at most 3 physical rows; earlier rows clip to available rows.

---
--- 3. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING — centred sliding window
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS centred_3row_sum
FROM sales
ORDER BY region, rep, sale_date;
-- Result: middle rows sum 3 values; first and last rows sum only 2 (frame clips at edges).

---
--- 4. RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND CURRENT ROW — date-based range
---    Includes all rows whose sale_date falls within the preceding 3 calendar days.
---    RANGE requires a single ORDER BY column of numeric or date type.
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND CURRENT ROW
    ) AS rolling_3day_sum
FROM sales
ORDER BY region, rep, sale_date;
-- Result: uses date distance, not row count — gaps in dates mean fewer rows are included
--         compared to ROWS BETWEEN 2 PRECEDING AND CURRENT ROW.

---
--- 5. ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING — suffix (reverse-running) sum
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS suffix_sum
FROM sales
ORDER BY region, rep, sale_date;
-- Result: suffix_sum decreases as we move forward; first row = partition total, last row = amount.

---
--- 6. ROWS vs RANGE comparison — showing the difference when ORDER BY has ties
---    Introduce tied sale_dates so RANGE widens the frame while ROWS stays physical.
---

WITH tied AS (
    SELECT region, rep, sale_date, amount FROM sales
    UNION ALL
    -- add a duplicate date for Alice/North on 2024-01-05 with a different amount
    SELECT 'North' AS region, 'Alice' AS rep, DATE '2024-01-05' AS sale_date, 50 AS amount
)

SELECT
    region,
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS rows_running_total,
    SUM(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS range_running_total
FROM tied
WHERE region = 'North' AND rep = 'Alice'
ORDER BY sale_date, amount;
-- Result: on the tied date (2024-01-05) RANGE includes BOTH tied rows in every peer's frame,
--         so range_running_total is the same for both rows on that date.
--         ROWS processes each physical row independently, giving different running totals.
