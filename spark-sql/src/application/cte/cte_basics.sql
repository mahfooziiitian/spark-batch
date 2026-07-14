-- Demonstrates Common Table Expressions (CTEs) from basic to multi-CTE patterns.

-- =============================================================================
-- Section 1: Basic CTE — Single WITH Block
-- =============================================================================

-- A CTE (WITH block) names a sub-result for reuse within the same query.

WITH high_value_sales AS (
    SELECT
        makename,
        saleprice,
        saledate,
        customername
    FROM allsales
    WHERE saleprice > 100000
)

SELECT
    makename,
    COUNT(*) AS sale_count,
    ROUND(AVG(saleprice), 2) AS avg_price
FROM high_value_sales
GROUP BY makename
ORDER BY avg_price DESC;

-- =============================================================================
-- Section 2: Calculate Averages Across Multiple Groupings
-- =============================================================================

WITH make_averages AS (
    SELECT
        makename,
        ROUND(AVG(saleprice), 2) AS avg_price,
        COUNT(*) AS sale_count,
        ROUND(AVG(saleprice - cost), 2) AS avg_margin
    FROM allsales
    GROUP BY makename
)

SELECT
    makename,
    avg_price,
    sale_count,
    avg_margin
FROM make_averages
ORDER BY avg_price DESC;

-- =============================================================================
-- Section 3: Reusing a CTE Twice in the Same Query
-- =============================================================================

WITH make_stats AS (
    SELECT
        makename,
        AVG(saleprice) AS avg_price,
        MAX(saleprice) AS max_price,
        MIN(saleprice) AS min_price
    FROM allsales
    GROUP BY makename
)

SELECT
    a.makename,
    a.saleprice,
    ms.avg_price,
    ms.max_price,
    ms.min_price,
    a.saleprice - ms.avg_price AS vs_avg
FROM allsales AS a
INNER JOIN make_stats AS ms
    ON a.makename = ms.makename
ORDER BY a.makename, a.saleprice;

-- =============================================================================
-- Section 4: Multiple CTEs in a Single Query
-- =============================================================================

-- Separate CTEs are comma-separated under a single WITH keyword.

WITH high_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS total_revenue
    FROM allsales
    WHERE saleprice > 80000
    GROUP BY makename
),

sale_counts AS (
    SELECT
        makename,
        COUNT(*) AS total_units
    FROM allsales
    GROUP BY makename
)

SELECT
    sc.makename,
    sc.total_units,
    COALESCE(hs.total_revenue, 0) AS high_value_revenue
FROM sale_counts AS sc
LEFT JOIN high_sales AS hs
    ON sc.makename = hs.makename
ORDER BY sc.makename;

-- =============================================================================
-- Section 5: CTE at Different Level of Detail
-- =============================================================================

-- CTE pre-aggregates to make level; main query enriches each sale row.

WITH make_stats AS (
    SELECT
        makename,
        ROUND(AVG(saleprice), 2) AS make_avg,
        SUM(saleprice) AS make_total,
        COUNT(*) AS make_count
    FROM allsales
    GROUP BY makename
)

SELECT
    a.makename,
    a.saleprice,
    a.saledate,
    ms.make_avg,
    ms.make_total,
    ms.make_count,
    a.saleprice - ms.make_avg AS vs_make_avg
FROM allsales AS a
INNER JOIN make_stats AS ms
    ON a.makename = ms.makename
ORDER BY a.makename, a.saleprice;

-- =============================================================================
-- Section 6: Sample Data
-- =============================================================================

WITH sample_sales AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        40000.00 AS cost
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        75000.00 AS saleprice,
        48000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        65000.00 AS cost
    UNION ALL
    SELECT
        'Bentley' AS makename,
        95000.00 AS saleprice,
        70000.00 AS cost
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice,
        80000.00 AS cost
),

make_summary AS (
    SELECT
        makename,
        ROUND(AVG(saleprice), 2) AS avg_price,
        COUNT(*) AS units
    FROM sample_sales
    GROUP BY makename
)

SELECT
    ms.makename,
    ms.avg_price,
    ms.units
FROM make_summary AS ms
ORDER BY ms.avg_price DESC;
