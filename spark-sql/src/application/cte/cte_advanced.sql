-- Advanced CTE patterns: nested references, comparing disparate datasets, two aggregation levels.

-- =============================================================================
-- Section 1: CTE Feeding Another CTE
-- =============================================================================

WITH base AS (
    SELECT
        makename,
        saleprice,
        YEAR(saledate) AS sale_year
    FROM allsales
),

yearly_totals AS (
    SELECT
        makename,
        sale_year,
        SUM(saleprice) AS yearly_revenue
    FROM base
    GROUP BY makename, sale_year
),

make_totals AS (
    SELECT
        makename,
        SUM(yearly_revenue) AS total_revenue
    FROM yearly_totals
    GROUP BY makename
)

SELECT
    yt.makename,
    yt.sale_year,
    yt.yearly_revenue,
    mt.total_revenue,
    ROUND(yt.yearly_revenue * 100.0 / mt.total_revenue, 2) AS pct_of_make_total
FROM yearly_totals AS yt
INNER JOIN make_totals AS mt
    ON yt.makename = mt.makename
ORDER BY yt.makename, yt.sale_year;

-- =============================================================================
-- Section 2: Comparing UK vs Non-UK Sales
-- =============================================================================

WITH uk_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS uk_revenue,
        COUNT(*) AS uk_units
    FROM allsales
    WHERE country = 'United Kingdom'
    GROUP BY makename
),

non_uk_sales AS (
    SELECT
        makename,
        SUM(saleprice) AS non_uk_revenue,
        COUNT(*) AS non_uk_units
    FROM allsales
    WHERE country != 'United Kingdom'
    GROUP BY makename
)

SELECT
    COALESCE(uk.makename, nuk.makename) AS makename,
    COALESCE(uk.uk_revenue, 0) AS uk_revenue,
    COALESCE(uk.uk_units, 0) AS uk_units,
    COALESCE(nuk.non_uk_revenue, 0) AS non_uk_revenue,
    COALESCE(nuk.non_uk_units, 0) AS non_uk_units
FROM uk_sales AS uk
FULL JOIN non_uk_sales AS nuk
    ON uk.makename = nuk.makename
ORDER BY COALESCE(uk.makename, nuk.makename);

-- =============================================================================
-- Section 3: Two Different Aggregation Levels from One CTE
-- =============================================================================

WITH monthly_base AS (
    SELECT
        makename,
        DATE_TRUNC('MONTH', saledate) AS sale_month,
        SUM(saleprice) AS monthly_revenue
    FROM allsales
    GROUP BY makename, DATE_TRUNC('MONTH', saledate)
),

quarterly_totals AS (
    SELECT
        makename,
        DATE_TRUNC('QUARTER', sale_month) AS sale_quarter,
        SUM(monthly_revenue) AS quarterly_revenue
    FROM monthly_base
    GROUP BY makename, DATE_TRUNC('QUARTER', sale_month)
)

SELECT
    mb.makename,
    mb.sale_month,
    mb.monthly_revenue,
    qt.sale_quarter,
    qt.quarterly_revenue,
    ROUND(mb.monthly_revenue * 100.0 / qt.quarterly_revenue, 2) AS pct_of_quarter
FROM monthly_base AS mb
INNER JOIN quarterly_totals AS qt
    ON mb.makename = qt.makename
    AND DATE_TRUNC('QUARTER', mb.sale_month) = qt.sale_quarter
ORDER BY mb.makename, mb.sale_month;

-- =============================================================================
-- Section 4: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        'United Kingdom' AS country
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        75000.00 AS saleprice,
        'France' AS country
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        'United Kingdom' AS country
    UNION ALL
    SELECT
        'Bentley' AS makename,
        95000.00 AS saleprice,
        'Germany' AS country
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice,
        'United Kingdom' AS country
),

uk_data AS (
    SELECT
        makename,
        SUM(saleprice) AS uk_revenue,
        COUNT(*) AS uk_units
    FROM sample_data
    WHERE country = 'United Kingdom'
    GROUP BY makename
),

non_uk_data AS (
    SELECT
        makename,
        SUM(saleprice) AS non_uk_revenue,
        COUNT(*) AS non_uk_units
    FROM sample_data
    WHERE country != 'United Kingdom'
    GROUP BY makename
)

SELECT
    COALESCE(uk.makename, nuk.makename) AS makename,
    COALESCE(uk.uk_revenue, 0) AS uk_revenue,
    COALESCE(uk.uk_units, 0) AS uk_units,
    COALESCE(nuk.non_uk_revenue, 0) AS non_uk_revenue,
    COALESCE(nuk.non_uk_units, 0) AS non_uk_units
FROM uk_data AS uk
FULL JOIN non_uk_data AS nuk
    ON uk.makename = nuk.makename
ORDER BY COALESCE(uk.makename, nuk.makename);
