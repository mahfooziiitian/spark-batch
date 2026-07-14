-- Demonstrates RANK() vs DENSE_RANK() side-by-side, multiple groupings,
-- subgroups, and filtering by rank. Ties show RANK gap vs DENSE_RANK no-gap.
-- Schema: allsales(makename, color, saleprice, saledate)

-- =============================================================================
-- Section 1: RANK() vs DENSE_RANK() side by side — tie behavior
-- =============================================================================
SELECT
    makename,
    saleprice,
    RANK() OVER (ORDER BY saleprice DESC) AS price_rank,
    DENSE_RANK() OVER (ORDER BY saleprice DESC) AS price_dense_rank
FROM allsales
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 2: Multiple groupings — overall rank AND per-make rank in one query
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    RANK() OVER (ORDER BY saleprice DESC) AS overall_rank,
    RANK() OVER (PARTITION BY makename ORDER BY saleprice DESC) AS make_rank
FROM allsales
ORDER BY overall_rank;

-- =============================================================================
-- Section 3: Subgroups — RANK() within makename+color partition
-- =============================================================================
SELECT
    makename,
    color,
    saleprice,
    RANK() OVER (PARTITION BY makename, color ORDER BY saleprice DESC) AS color_rank
FROM allsales
ORDER BY makename ASC, color ASC, saleprice DESC;

-- =============================================================================
-- Section 4: Filter to top 3 per make (WHERE make_rank <= 3)
-- =============================================================================
WITH ranked_sales AS (
    SELECT
        makename,
        saledate,
        saleprice,
        RANK() OVER (PARTITION BY makename ORDER BY saleprice DESC) AS make_rank
    FROM allsales
)

SELECT
    makename,
    saledate,
    saleprice,
    make_rank
FROM ranked_sales
WHERE make_rank <= 3
ORDER BY makename, make_rank;

-- =============================================================================
-- Section 5: Sample data with deliberate ties showing RANK gap vs DENSE_RANK
-- =============================================================================
-- saleprice   price_rank  price_dense_rank
-- 115000.00   1           1
-- 95000.00    2           2
-- 90000.00    3           3
-- 90000.00    3           3   (tie: same RANK, same DENSE_RANK)
-- 80000.00    5           4   (RANK skips 4, DENSE_RANK continues from 4)
-- 78000.00    6           5
-- 72000.00    7           6
-- 65000.00    8           7
-- 55000.00    9           8
