-- Advanced correlated subquery patterns: EXISTS, NOT EXISTS, filter on aggregate, duplicating results.

-- =============================================================================
-- Section 1: EXISTS — Customers Who Bought at Least One Ferrari
-- =============================================================================

SELECT DISTINCT customername
FROM allsales AS a
WHERE
    EXISTS (
        SELECT 1
        FROM allsales AS a2
        WHERE
            a2.customername = a.customername
            AND a2.makename = 'Ferrari'
    )
ORDER BY customername;

-- =============================================================================
-- Section 2: NOT EXISTS — Customers Who Never Bought a Ferrari
-- =============================================================================

SELECT DISTINCT customername
FROM allsales AS a
WHERE
    NOT EXISTS (
        SELECT 1
        FROM allsales AS a2
        WHERE
            a2.customername = a.customername
            AND a2.makename = 'Ferrari'
    )
ORDER BY customername;

-- =============================================================================
-- Section 3: Filter on Aggregate — Sales Where Customer Average > 80000
-- =============================================================================

SELECT
    customername,
    makename,
    saleprice
FROM allsales AS a
WHERE (
    SELECT AVG(a2.saleprice)
    FROM allsales AS a2
    WHERE a2.customername = a.customername
) > 80000
ORDER BY customername;

-- =============================================================================
-- Section 4: Correlated Result in Output — Sale vs Make Max Price
-- =============================================================================

SELECT
    a.makename,
    a.saleprice,
    (
        SELECT MAX(a2.saleprice)
        FROM allsales AS a2
        WHERE a2.makename = a.makename
    ) AS make_max_price,
    a.saleprice = (
        SELECT MAX(a2.saleprice)
        FROM allsales AS a2
        WHERE a2.makename = a.makename
    ) AS is_top_sale
FROM allsales AS a
ORDER BY a.makename ASC, a.saleprice DESC;

-- =============================================================================
-- Section 5: Window Function Equivalent for Section 4
-- =============================================================================

SELECT
    makename,
    saleprice,
    MAX(saleprice) OVER (PARTITION BY makename) AS make_max_price,
    saleprice = MAX(saleprice) OVER (PARTITION BY makename) AS is_top_sale
FROM allsales
ORDER BY makename ASC, saleprice DESC;

-- =============================================================================
-- Section 6: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        'Alice' AS customername
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        80000.00 AS saleprice,
        'Bob' AS customername
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        'Alice' AS customername
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice,
        'Carol' AS customername
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        140000.00 AS saleprice,
        'Bob' AS customername
)

SELECT
    makename,
    customername,
    saleprice,
    MAX(saleprice) OVER (PARTITION BY makename) AS make_max_price,
    saleprice = MAX(saleprice) OVER (PARTITION BY makename) AS is_top_sale
FROM sample_data
ORDER BY makename ASC, saleprice DESC;
