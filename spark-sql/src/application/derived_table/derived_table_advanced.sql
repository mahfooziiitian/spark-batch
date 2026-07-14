-- Advanced derived table patterns: year-over-year, joining unconnected tables, filter synchronization.

-- =============================================================================
-- Section 1: Year-Over-Year Comparison
-- =============================================================================

WITH current_year AS (
    SELECT
        makename,
        SUM(saleprice) AS current_revenue
    FROM allsales
    WHERE YEAR(saledate) = 2018
    GROUP BY makename
),

prior_year AS (
    SELECT
        makename,
        SUM(saleprice) AS prior_revenue
    FROM allsales
    WHERE YEAR(saledate) = 2017
    GROUP BY makename
)

SELECT
    COALESCE(cy.makename, py.makename) AS makename,
    COALESCE(cy.current_revenue, 0) AS revenue_2018,
    COALESCE(py.prior_revenue, 0) AS revenue_2017,
    COALESCE(cy.current_revenue, 0) - COALESCE(py.prior_revenue, 0) AS yoy_change,
    ROUND(
        (COALESCE(cy.current_revenue, 0) - COALESCE(py.prior_revenue, 0))
        * 100.0 / NULLIF(py.prior_revenue, 0),
        2
    ) AS yoy_pct_change
FROM current_year AS cy
FULL JOIN prior_year AS py
    ON cy.makename = py.makename
ORDER BY yoy_change DESC;

-- =============================================================================
-- Section 2: Joining Unconnected Tables via Derived Table
-- =============================================================================

WITH country_sales AS (
    SELECT
        country,
        COUNT(*) AS sale_count,
        SUM(saleprice) AS total_revenue
    FROM allsales
    GROUP BY country
),

country_stats AS (
    SELECT
        'United Kingdom' AS country,
        67000000 AS population
    UNION ALL
    SELECT
        'France' AS country,
        68000000 AS population
    UNION ALL
    SELECT
        'Germany' AS country,
        84000000 AS population
    UNION ALL
    SELECT
        'USA' AS country,
        335000000 AS population
)

SELECT
    cs.country,
    cs.sale_count,
    cs.total_revenue,
    cst.population,
    ROUND(cs.total_revenue / cst.population, 4) AS revenue_per_capita
FROM country_sales AS cs
INNER JOIN country_stats AS cst
    ON cs.country = cst.country
ORDER BY revenue_per_capita DESC;

-- =============================================================================
-- Section 3: Synchronizing Filters Between Derived Table and Main Query
-- =============================================================================

WITH filtered_base AS (
    SELECT
        makename,
        color,
        saleprice,
        saledate
    FROM allsales
    WHERE saledate BETWEEN TO_DATE('2017-01-01') AND TO_DATE('2017-12-31')
),

make_avg_2017 AS (
    SELECT
        makename,
        ROUND(AVG(saleprice), 2) AS avg_2017
    FROM filtered_base
    GROUP BY makename
)

SELECT
    fb.makename,
    fb.color,
    fb.saleprice,
    ma.avg_2017,
    fb.saleprice - ma.avg_2017 AS vs_2017_avg
FROM filtered_base AS fb
INNER JOIN make_avg_2017 AS ma
    ON fb.makename = ma.makename
ORDER BY fb.makename ASC, fb.saleprice DESC;

-- =============================================================================
-- Section 4: Multiple Derived Tables for Quarterly Pivot
-- =============================================================================

WITH q1_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS q1_revenue
    FROM allsales
    WHERE MONTH(saledate) BETWEEN 1 AND 3
    GROUP BY makename
),

q2_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS q2_revenue
    FROM allsales
    WHERE MONTH(saledate) BETWEEN 4 AND 6
    GROUP BY makename
),

q3_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS q3_revenue
    FROM allsales
    WHERE MONTH(saledate) BETWEEN 7 AND 9
    GROUP BY makename
),

q4_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS q4_revenue
    FROM allsales
    WHERE MONTH(saledate) BETWEEN 10 AND 12
    GROUP BY makename
)

SELECT
    COALESCE(q1.makename, q2.makename, q3.makename, q4.makename) AS makename,
    COALESCE(q1.q1_revenue, 0) AS q1,
    COALESCE(q2.q2_revenue, 0) AS q2,
    COALESCE(q3.q3_revenue, 0) AS q3,
    COALESCE(q4.q4_revenue, 0) AS q4
FROM q1_sales AS q1
FULL JOIN q2_sales AS q2 ON q1.makename = q2.makename
FULL JOIN q3_sales AS q3 ON COALESCE(q1.makename, q2.makename) = q3.makename
FULL JOIN q4_sales AS q4 ON COALESCE(q1.makename, q2.makename, q3.makename) = q4.makename
ORDER BY makename;

-- =============================================================================
-- Section 5: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        TO_DATE('2017-02-10') AS saledate,
        'Red' AS color
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        75000.00 AS saleprice,
        TO_DATE('2017-07-15') AS saledate,
        'Black' AS color
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        TO_DATE('2017-04-20') AS saledate,
        'Silver' AS color
    UNION ALL
    SELECT
        'Bentley' AS makename,
        95000.00 AS saleprice,
        TO_DATE('2018-01-05') AS saledate,
        'Blue' AS color
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice,
        TO_DATE('2017-11-30') AS saledate,
        'Yellow' AS color
),

make_avg_2017 AS (
    SELECT
        makename,
        ROUND(AVG(saleprice), 2) AS avg_2017
    FROM sample_data
    WHERE YEAR(saledate) = 2017
    GROUP BY makename
)

SELECT
    sd.makename,
    sd.color,
    sd.saleprice,
    COALESCE(ma.avg_2017, 0) AS avg_2017,
    sd.saleprice - COALESCE(ma.avg_2017, 0) AS vs_avg
FROM sample_data AS sd
LEFT JOIN make_avg_2017 AS ma
    ON sd.makename = ma.makename
ORDER BY sd.makename ASC, sd.saleprice DESC;
