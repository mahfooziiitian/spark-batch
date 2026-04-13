-- Demonstrates CUME_DIST() and PERCENT_RANK() window functions for
-- distribution analysis and percentile banding.
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: CUME_DIST() — fraction of rows at or below current saleprice
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(CUME_DIST() OVER (ORDER BY saleprice), 4) AS cume_dist_val
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 2: PERCENT_RANK() — relative rank from 0.0 to 1.0
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(PERCENT_RANK() OVER (ORDER BY saleprice), 4) AS percent_rank_val
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 3: Both together with CASE percentile band
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(CUME_DIST() OVER (ORDER BY saleprice), 4) AS cume_dist_val,
    ROUND(PERCENT_RANK() OVER (ORDER BY saleprice), 4) AS percent_rank_val,
    CASE
        WHEN CUME_DIST() OVER (ORDER BY saleprice) >= 0.90 THEN 'Top 10%'
        WHEN CUME_DIST() OVER (ORDER BY saleprice) >= 0.75 THEN 'Top 25%'
        WHEN CUME_DIST() OVER (ORDER BY saleprice) >= 0.50 THEN 'Top 50%'
        ELSE 'Bottom 50%'
    END AS percentile_band
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 4: CUME_DIST within each make using PARTITION BY makename
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    ROUND(CUME_DIST() OVER (PARTITION BY makename ORDER BY saleprice), 4) AS make_cume_dist
FROM allsales
ORDER BY makename, saleprice;

-- =============================================================================
-- Section 5: Sample data with 8 rows showing exact values
-- =============================================================================
-- saleprice   cume_dist_val  percent_rank_val  percentile_band
-- 55000.00    0.1250         0.0000            Bottom 50%
-- 65000.00    0.2500         0.1429            Bottom 50%
-- 72000.00    0.3750         0.2857            Bottom 50%
-- 78000.00    0.5000         0.4286            Top 50%
-- 80000.00    0.6250         0.5714            Top 50%
-- 90000.00    0.7500         0.7143            Top 25%
-- 95000.00    0.8750         0.8571            Top 25%
-- 115000.00   1.0000         1.0000            Top 10%
