-- Uses derived tables (subqueries in FROM) to create intermediate result sets
-- for multi-step calculations.

-- =============================================================================
-- Section 1: Pre-Calculate Make Totals, Then Filter
-- =============================================================================

SELECT
    make_totals.makename,
    make_totals.total_revenue
FROM ( -- noqa: ST05
    SELECT
        makename,
        SUM(saleprice) AS total_revenue
    FROM allsales
    GROUP BY makename
) AS make_totals
WHERE make_totals.total_revenue > 200000
ORDER BY make_totals.total_revenue DESC;

-- =============================================================================
-- Section 2: Custom Classification Using Derived Table
-- =============================================================================

SELECT
    classified.makename,
    classified.saleprice,
    classified.price_tier
FROM ( -- noqa: ST05
    SELECT
        makename,
        saleprice,
        CASE
            WHEN saleprice >= 100000 THEN 'Premium'
            WHEN saleprice >= 60000 THEN 'Mid-Range'
            ELSE 'Entry'
        END AS price_tier
    FROM allsales
) AS classified
ORDER BY classified.price_tier ASC, classified.saleprice DESC;

-- =============================================================================
-- Section 3: Joining Derived Table with Real Table (CTE Version)
-- =============================================================================

WITH make_avg AS (
    SELECT
        makename,
        ROUND(AVG(saleprice), 2) AS avg_price
    FROM allsales
    GROUP BY makename
)

SELECT
    a.makename,
    a.saleprice,
    ma.avg_price,
    a.saleprice - ma.avg_price AS vs_avg
FROM allsales AS a
INNER JOIN make_avg AS ma
    ON a.makename = ma.makename
ORDER BY a.makename, a.saleprice;

-- =============================================================================
-- Section 4: Joining Multiple Derived Tables (Two CTEs Joined)
-- =============================================================================

WITH make_revenue AS (
    SELECT
        makename,
        SUM(saleprice) AS total_revenue
    FROM allsales
    GROUP BY makename
),

make_units AS (
    SELECT
        makename,
        COUNT(*) AS total_units
    FROM allsales
    GROUP BY makename
)

SELECT
    mr.makename,
    mr.total_revenue,
    mu.total_units,
    ROUND(mr.total_revenue / mu.total_units, 2) AS revenue_per_unit
FROM make_revenue AS mr
INNER JOIN make_units AS mu
    ON mr.makename = mu.makename
ORDER BY revenue_per_unit DESC;

-- =============================================================================
-- Section 5: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        75000.00 AS saleprice
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice
    UNION ALL
    SELECT
        'Bentley' AS makename,
        95000.00 AS saleprice
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice
),

make_totals AS (
    SELECT
        makename,
        SUM(saleprice) AS total_revenue,
        COUNT(*) AS units
    FROM sample_data
    GROUP BY makename
)

SELECT
    makename,
    total_revenue,
    units,
    ROUND(total_revenue / units, 2) AS revenue_per_unit
FROM make_totals
ORDER BY total_revenue DESC;
