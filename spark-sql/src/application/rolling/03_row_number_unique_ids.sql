-- Demonstrates ROW_NUMBER() for surrogate IDs, within-partition numbering,
-- deduplication, and ID generation after aggregation.
-- Schema: allsales(makename, saleprice, saledate, customername)

-- =============================================================================
-- Section 1: Sequential sale_id ordered by saledate
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROW_NUMBER() OVER (ORDER BY saledate) AS sale_id
FROM allsales
ORDER BY sale_id;

-- =============================================================================
-- Section 2: sale_within_make — per-make sequential numbering by saledate
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    ROW_NUMBER() OVER (PARTITION BY makename ORDER BY saledate) AS sale_within_make
FROM allsales
ORDER BY makename, saledate;

-- =============================================================================
-- Section 3: Keep only most recent sale per make (deduplication via CTE)
-- =============================================================================
WITH rn_desc AS (
    SELECT
        makename,
        saledate,
        saleprice,
        ROW_NUMBER() OVER (PARTITION BY makename ORDER BY saledate DESC) AS rn
    FROM allsales
)

SELECT
    makename,
    saledate,
    saleprice
FROM rn_desc
WHERE rn = 1
ORDER BY makename;

-- =============================================================================
-- Section 4: Apply ROW_NUMBER after aggregation (ranked make totals with ID)
-- =============================================================================
WITH make_totals AS (
    SELECT
        makename,
        SUM(saleprice) AS total_revenue,
        COUNT(*) AS sale_count
    FROM allsales
    GROUP BY makename
)

SELECT
    makename,
    total_revenue,
    sale_count,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM make_totals
ORDER BY revenue_rank;

-- =============================================================================
-- Section 5: Sample data demo — per-make numbering
-- =============================================================================
-- makename    saledate    saleprice  sale_within_make
-- Ferrari     2017-01-01  65000.00   1
-- Ferrari     2017-01-04  72000.00   2
-- Ferrari     2017-02-10  68000.00   3
-- Bentley     2017-01-02  90000.00   1
-- Bentley     2017-01-06  80000.00   2
-- Bentley     2017-03-15  95000.00   3
