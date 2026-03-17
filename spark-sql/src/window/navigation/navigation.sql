-- Navigation window functions: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE.
-- Demonstrates period-over-period deltas, day-over-day % change, and boundary value retrieval.

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
--- 1. LAG and LEAD side-by-side — previous and next sale amount per rep within region
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    LAG(amount, 1)
        OVER (PARTITION BY region, rep ORDER BY sale_date)
        AS prev_amount,
    LEAD(amount, 1)
        OVER (PARTITION BY region, rep ORDER BY sale_date)
        AS next_amount
FROM sales
ORDER BY region, rep, sale_date;
-- Result: first row per rep has prev_amount = NULL; last row has next_amount = NULL.

---
--- 2. LAG with default value — replace leading NULL with 0 (third parameter)
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    LAG(amount, 1, 0)
        OVER (PARTITION BY region, rep ORDER BY sale_date)
        AS prev_amount_default
FROM sales
ORDER BY region, rep, sale_date;
-- Result: first row per rep shows 0 instead of NULL.

---
--- 3. Period-over-period delta: amount minus previous amount
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    amount
    - LAG(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date) AS amount_delta
FROM sales
ORDER BY region, rep, sale_date;
-- Result: amount_delta is NULL for the first sale, positive/negative for subsequent ones.

---
--- 4. FIRST_VALUE and LAST_VALUE per (region, rep)
---    LAST_VALUE requires an explicit full-partition frame to see the true last row.
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_amount
FROM sales
ORDER BY region, rep, sale_date;
-- Result: first_amount = earliest sale, last_amount = latest sale — same on every row of the group.

---
--- 5. NTH_VALUE — second sale amount per (region, rep) using a full-partition frame
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    NTH_VALUE(amount, 2) OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_amount
FROM sales
ORDER BY region, rep, sale_date;
-- Result: NULL until at least 2 rows exist in the partition; then the second sale value repeats.

---
--- 6. Day-over-day % change (rounded to 2 dp)
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    LAG(amount, 1)
        OVER (PARTITION BY region, rep ORDER BY sale_date)
        AS prev_amount,
    ROUND(
        (
            amount
            - LAG(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date)
        )
        / LAG(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date)
        * 100,
        2
    ) AS pct_change
FROM sales
ORDER BY region, rep, sale_date;
-- Result: pct_change is NULL for the first row; e.g. 100 → 200 yields 100.00 %.
