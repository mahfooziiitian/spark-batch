-- NULL handling in window functions: RESPECT NULLS vs IGNORE NULLS with FIRST_VALUE,
-- LAST_VALUE, LAG, NTH_VALUE, and COALESCE-based forward-fill patterns.

-- Extend the shared sales dataset with deliberate NULLs in the amount column
-- to demonstrate how each function handles missing values.
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

-- Overlay NULLs: replace specific amounts with NULL to simulate missing data.
CREATE OR REPLACE TEMP VIEW sales_with_nulls AS
SELECT
    region,
    rep,
    sale_date,
    CASE
        WHEN rep = 'Alice' AND sale_date = DATE '2024-01-05' THEN NULL
        WHEN rep = 'Bob' AND sale_date = DATE '2024-01-06' THEN NULL
        ELSE amount
    END AS amount
FROM sales;

---
--- 1. FIRST_VALUE RESPECT NULLS (default) vs IGNORE NULLS
---    RESPECT NULLS: the first physical value in the frame — may be NULL.
---    IGNORE NULLS:  skips NULLs and returns the first non-NULL value.
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    FIRST_VALUE(amount) IGNORE NULLS OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_non_null_amount
FROM sales_with_nulls
ORDER BY region, rep, sale_date;
-- Result: first_non_null_amount skips NULL rows and returns the earliest real value.

---
--- 2. LAST_VALUE IGNORE NULLS — forward-fill (last non-null seen so far)
---    Use a growing frame so "last non-null so far" is carried forward.
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    LAST_VALUE(amount) IGNORE NULLS OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS forward_filled_amount
FROM sales_with_nulls
ORDER BY region, rep, sale_date;
-- Result: NULL rows receive the last known non-null value from preceding rows.
-- This is the canonical SQL forward-fill pattern.

---
--- 3. LAG IGNORE NULLS — skip NULL rows when looking back
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    LAG(amount, 1) IGNORE NULLS OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
    ) AS prev_non_null_amount
FROM sales_with_nulls
ORDER BY region, rep, sale_date;
-- Result: the row after a NULL does not see NULL as its predecessor;
--         it skips back to the nearest prior non-null value instead.

---
--- 4. NTH_VALUE IGNORE NULLS — second non-null amount per (region, rep)
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    NTH_VALUE(amount, 2) IGNORE NULLS OVER (
        PARTITION BY region, rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_non_null_amount
FROM sales_with_nulls
ORDER BY region, rep, sale_date;
-- Result: NULLs are excluded from the N-count; the result is the second real value in the group.

---
--- 5. COALESCE with LAG to handle leading NULLs from the window boundary
---    When the frame has no preceding row, LAG returns NULL.
---    COALESCE replaces that boundary NULL with a meaningful sentinel (0 here).
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    COALESCE(
        LAG(amount, 1) OVER (PARTITION BY region, rep ORDER BY sale_date),
        0
    ) AS prev_amount_or_zero
FROM sales_with_nulls
ORDER BY region, rep, sale_date;
-- Result: the first row per (region, rep) shows 0 instead of NULL.
-- Note: COALESCE cannot distinguish a genuine NULL data value from a boundary NULL;
--       use LAG IGNORE NULLS (example 3) when the data itself can be NULL.
