-- Joins tables using range conditions (BETWEEN, >, <) rather than equality,
-- useful for banding and lookup tables.

-- =============================================================================
-- Section 1: Price Band Lookup Join
-- =============================================================================

WITH price_bands AS (
    SELECT
        'Entry' AS band_name,
        0 AS band_min,
        59999 AS band_max
    UNION ALL
    SELECT
        'Mid-Range' AS band_name,
        60000 AS band_min,
        99999 AS band_max
    UNION ALL
    SELECT
        'Premium' AS band_name,
        100000 AS band_min,
        149999 AS band_max
    UNION ALL
    SELECT
        'Ultra' AS band_name,
        150000 AS band_min,
        999999 AS band_max
)

SELECT
    a.makename,
    a.saleprice,
    pb.band_name
FROM allsales AS a
INNER JOIN price_bands AS pb
    ON a.saleprice BETWEEN pb.band_min AND pb.band_max
ORDER BY a.saleprice;

-- =============================================================================
-- Section 2: Date Range Lookup — Quarter Assignment
-- =============================================================================

WITH quarters AS (
    SELECT
        1 AS quarter_num,
        TO_DATE('2017-01-01') AS qtr_start,
        TO_DATE('2017-03-31') AS qtr_end
    UNION ALL
    SELECT
        2 AS quarter_num,
        TO_DATE('2017-04-01') AS qtr_start,
        TO_DATE('2017-06-30') AS qtr_end
    UNION ALL
    SELECT
        3 AS quarter_num,
        TO_DATE('2017-07-01') AS qtr_start,
        TO_DATE('2017-09-30') AS qtr_end
    UNION ALL
    SELECT
        4 AS quarter_num,
        TO_DATE('2017-10-01') AS qtr_start,
        TO_DATE('2017-12-31') AS qtr_end
)

SELECT
    a.saledate,
    a.makename,
    a.saleprice,
    q.quarter_num
FROM allsales AS a
INNER JOIN quarters AS q
    ON a.saledate BETWEEN q.qtr_start AND q.qtr_end
WHERE YEAR(a.saledate) = 2017
ORDER BY a.saledate;

-- =============================================================================
-- Section 3: Non-Equi Join — Lower-Priced Sales of the Same Make
-- =============================================================================

SELECT
    a1.makename,
    a1.saleprice AS higher_price,
    a2.saleprice AS lower_price,
    a1.saleprice - a2.saleprice AS price_gap
FROM allsales AS a1
INNER JOIN allsales AS a2
    ON
        a1.makename = a2.makename
        AND a1.saleprice > a2.saleprice
ORDER BY a1.makename ASC, price_gap DESC;

-- =============================================================================
-- Section 4: Sample Data with Expected Output
-- =============================================================================

WITH sample_sales AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        45000.00 AS saleprice
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        155000.00 AS saleprice
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice
),

price_bands AS (
    SELECT
        'Entry' AS band_name,
        0 AS band_min,
        59999 AS band_max
    UNION ALL
    SELECT
        'Mid-Range' AS band_name,
        60000 AS band_min,
        99999 AS band_max
    UNION ALL
    SELECT
        'Premium' AS band_name,
        100000 AS band_min,
        149999 AS band_max
    UNION ALL
    SELECT
        'Ultra' AS band_name,
        150000 AS band_min,
        999999 AS band_max
)

SELECT
    ss.makename,
    ss.saleprice,
    pb.band_name
FROM sample_sales AS ss
INNER JOIN price_bands AS pb
    ON ss.saleprice BETWEEN pb.band_min AND pb.band_max
ORDER BY ss.saleprice;
