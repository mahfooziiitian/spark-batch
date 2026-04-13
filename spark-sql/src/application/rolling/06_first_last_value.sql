-- Demonstrates FIRST_VALUE and LAST_VALUE window functions.
-- LAST_VALUE requires ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
-- to return the true last value (default frame stops at current row).
-- Schema: allsales(makename, saleprice, saledate, customername)

-- =============================================================================
-- Section 1: FIRST_VALUE per make partition
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    FIRST_VALUE(saleprice) OVER (
        PARTITION BY makename
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS first_sale_price
FROM allsales
ORDER BY makename, saledate;

-- =============================================================================
-- Section 2: LAST_VALUE per make partition (correct frame for true last value)
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    LAST_VALUE(saleprice) OVER (
        PARTITION BY makename
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_sale_price
FROM allsales
ORDER BY makename, saledate;

-- =============================================================================
-- Section 3: Named WINDOW combining both + price_movement = last - first
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    FIRST_VALUE(saleprice) OVER full_w AS first_price,
    LAST_VALUE(saleprice) OVER full_w AS last_price,
    LAST_VALUE(saleprice) OVER full_w - FIRST_VALUE(saleprice) OVER full_w AS price_movement
FROM allsales
WINDOW full_w AS (
    PARTITION BY makename
    ORDER BY saledate
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY makename, saledate;

-- =============================================================================
-- Section 4: First sale AND last 4 sales per client using two ROW_NUMBER calls
-- =============================================================================
WITH ranked AS (
    SELECT
        customername,
        saledate,
        makename,
        saleprice,
        ROW_NUMBER() OVER (PARTITION BY customername ORDER BY saledate ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY customername ORDER BY saledate DESC) AS rn_desc
    FROM allsales
)

SELECT
    customername,
    saledate,
    makename,
    saleprice,
    CASE
        WHEN rn_asc = 1 THEN 'First Sale'
        WHEN rn_desc <= 4 THEN 'Last 4 Sales'
        ELSE 'Other'
    END AS sale_category
FROM ranked
WHERE rn_asc = 1 OR rn_desc <= 4
ORDER BY customername, saledate;
