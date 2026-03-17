-- Ranking window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST.
-- Demonstrates tie behaviour, top-N per partition, de-duplication, and percentile scoring.

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
--- 1. ROW_NUMBER vs RANK vs DENSE_RANK — tie behaviour side-by-side
---    Introduce a tie by unioning a duplicate amount so the difference is visible.

WITH tied AS (
    SELECT region, rep, amount FROM sales
    UNION ALL
    -- duplicate Bob/North/250 to create a tie at rank 1 within North for Bob
    SELECT 'North' AS region, 'Bob' AS rep, 250 AS amount
)

SELECT
    region,
    rep,
    amount,
    -- unique, arbitrary tiebreak
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
    -- gaps after ties
    RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk,
    -- no gaps
    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS drnk
FROM tied
ORDER BY region ASC, amount DESC;
-- Result: tied rows share the same RANK/DENSE_RANK; ROW_NUMBER is always unique.
-- RANK gaps: 1,1,3 — DENSE_RANK no gaps: 1,1,2.

---
--- 2. NTILE(4) — quartile bucketing within each region
---

SELECT
    region,
    rep,
    sale_date,
    amount,
    NTILE(4) OVER (PARTITION BY region ORDER BY amount) AS quartile
FROM sales
ORDER BY region, amount;
-- Result: rows split into four equal-ish buckets per region (Q1 = lowest, Q4 = highest).

---
--- 3. PERCENT_RANK and CUME_DIST
---    PERCENT_RANK: (rank - 1) / (rows - 1)  → 0.0 … 1.0
---    CUME_DIST:    rows <= current / total   → (0, 1]

SELECT
    region,
    rep,
    amount,
    ROUND(PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount), 4)
        AS pct_rank,
    ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY amount), 4)
        AS cume_distribution
FROM sales
ORDER BY region, amount;
-- Result: first row per region always has pct_rank = 0.0; last row cume_distribution = 1.0.

---
--- 4. Top-N per partition — top 2 sales by amount within each region
---

WITH ranked AS (
    SELECT
        region,
        rep,
        sale_date,
        amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
    FROM sales
)

SELECT
    region,
    rep,
    sale_date,
    amount
FROM ranked
WHERE rn <= 2
ORDER BY region ASC, amount DESC;
-- Result: exactly 2 rows per region (highest and second-highest amount).

---
--- 5. De-duplication with ROW_NUMBER — keep the most recent sale per rep per region
---

WITH deduped AS (
    SELECT
        region,
        rep,
        sale_date,
        amount,
        ROW_NUMBER()
            OVER (PARTITION BY region, rep ORDER BY sale_date DESC)
            AS rn
    FROM sales
)

SELECT
    region,
    rep,
    sale_date,
    amount
FROM deduped
WHERE rn = 1
ORDER BY region, rep;
-- Result: one row per (region, rep) pair — the latest sale date wins.

---
--- 6. Percentile scoring with PERCENT_RANK across all reps (no partition)
---

SELECT
    rep,
    region,
    amount,
    ROUND(PERCENT_RANK() OVER (ORDER BY amount) * 100, 1) AS global_pct_score
FROM sales
ORDER BY amount;
-- Result: global_pct_score shows each sale's relative standing across the entire dataset.
-- The lowest amount scores 0.0; the highest scores 100.0.
