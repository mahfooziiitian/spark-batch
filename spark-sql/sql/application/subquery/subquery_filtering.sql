-- Uses subqueries in WHERE to filter rows based on external or aggregated conditions.

-- =============================================================================
-- Section 1: Subquery as Filter — Above Grand Average
-- =============================================================================

SELECT
    a.makename,
    a.saleprice
FROM allsales AS a
WHERE a.saleprice > (SELECT AVG(s.saleprice) FROM allsales AS s)
ORDER BY a.saleprice DESC;

-- =============================================================================
-- Section 2: IN Subquery — Makes from Italy
-- =============================================================================

SELECT
    a.makename,
    a.saleprice,
    a.saledate
FROM allsales AS a
WHERE
    a.makename IN (
        SELECT mk.makename
        FROM make AS mk
        WHERE mk.country = 'Italy'
    )
ORDER BY a.makename, a.saledate;

-- =============================================================================
-- Section 3: NOT IN with NOT EXISTS Alternative
-- =============================================================================

-- NOT IN has a NULL pitfall: if the subquery returns any NULL, the result is empty.
-- NOT EXISTS is safer.

SELECT DISTINCT a.customername
FROM allsales AS a
WHERE
    NOT EXISTS (
        SELECT 1
        FROM allsales AS a2
        WHERE
            a2.customername = a.customername
            AND a2.makename = 'Ferrari'
    )
ORDER BY a.customername;

-- =============================================================================
-- Section 4: Nested Subquery — Filter by Most Recent Year Average
-- =============================================================================

SELECT
    a.makename,
    a.saleprice,
    YEAR(a.saledate) AS sale_year
FROM allsales AS a
WHERE
    a.saleprice > (
        SELECT AVG(s.saleprice)
        FROM allsales AS s
        WHERE YEAR(s.saledate) = (
            SELECT MAX(YEAR(s2.saledate))
            FROM allsales AS s2
        )
    )
ORDER BY a.saleprice DESC;

-- =============================================================================
-- Section 5: BETWEEN Two Subquery Values
-- =============================================================================

SELECT
    a.makename,
    a.saleprice
FROM allsales AS a
WHERE
    a.saleprice BETWEEN
    (
        SELECT MIN(s.saleprice)
        FROM allsales AS s
        WHERE s.makename = 'Ferrari'
    )
    AND (
        SELECT MAX(s.saleprice)
        FROM allsales AS s
        WHERE s.makename = 'Bentley'
    )
ORDER BY a.saleprice;

-- =============================================================================
-- Section 6: Subquery in HAVING
-- =============================================================================

-- Filter after aggregation using a scalar subquery for the benchmark.

SELECT
    a.makename,
    AVG(a.saleprice) AS avg_price
FROM allsales AS a
GROUP BY a.makename
HAVING AVG(a.saleprice) > (SELECT AVG(s.saleprice) FROM allsales AS s)
ORDER BY avg_price DESC;

-- =============================================================================
-- Section 7: Separate Filters — Main vs Subquery
-- =============================================================================

-- Main query filters to 2017; subquery benchmarks against 2016 average.

SELECT
    a.makename,
    a.saleprice,
    YEAR(a.saledate) AS sale_year
FROM allsales AS a
WHERE
    YEAR(a.saledate) = 2017
    AND a.saleprice > (
        SELECT AVG(s.saleprice)
        FROM allsales AS s
        WHERE YEAR(s.saledate) = 2016
    )
ORDER BY a.makename, a.saleprice;

-- =============================================================================
-- Section 8: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        TO_DATE('2017-03-15') AS saledate
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        80000.00 AS saleprice,
        TO_DATE('2016-06-20') AS saledate
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        TO_DATE('2017-09-10') AS saledate
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        140000.00 AS saleprice,
        TO_DATE('2017-01-05') AS saledate
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        55000.00 AS saleprice,
        TO_DATE('2016-11-22') AS saledate
)

SELECT
    sd.makename,
    sd.saleprice,
    sd.saledate
FROM sample_data AS sd
WHERE sd.saleprice > (SELECT AVG(s.saleprice) FROM sample_data AS s)
ORDER BY sd.saleprice DESC;
