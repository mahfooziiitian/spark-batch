-- Embeds subqueries in SELECT to add aggregated context to each detail row.

-- =============================================================================
-- Section 1: Grand Average on Every Row
-- =============================================================================

SELECT
    a.makename,
    a.saleprice,
    ROUND((SELECT AVG(s.saleprice) FROM allsales AS s), 2) AS grand_avg,
    a.saleprice - ROUND((SELECT AVG(s.saleprice) FROM allsales AS s), 2) AS vs_grand_avg
FROM allsales AS a
ORDER BY a.makename;

-- =============================================================================
-- Section 2: Percentage of Grand Total
-- =============================================================================

SELECT
    a.makename,
    a.saleprice,
    ROUND(a.saleprice * 100.0 / (SELECT SUM(s.saleprice) FROM allsales AS s), 2) AS pct_of_total
FROM allsales AS a
ORDER BY a.saleprice DESC;

-- =============================================================================
-- Section 3: Sales Above Second-Highest Make Average
-- =============================================================================

-- Nested subquery: outer gets the second highest avg; inner computes all make avgs.

SELECT
    makename,
    saleprice
FROM allsales
WHERE
    saleprice > (
        SELECT MAX(lower_avgs.avg_price)
        FROM (
            SELECT
                makename,
                AVG(saleprice) AS avg_price
            FROM allsales
            GROUP BY makename
            HAVING AVG(saleprice) < (
                SELECT MAX(all_avgs.avg_price2)
                FROM (
                    SELECT AVG(saleprice) AS avg_price2 -- noqa: ST05
                    FROM allsales
                    GROUP BY makename
                ) AS all_avgs
            )
        ) AS lower_avgs -- noqa: ST05
    )
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 4: HAVING with Subquery
-- =============================================================================

SELECT
    a.makename,
    AVG(a.saleprice) AS avg_price
FROM allsales AS a
GROUP BY a.makename
HAVING AVG(a.saleprice) > (SELECT AVG(s.saleprice) FROM allsales AS s)
ORDER BY avg_price DESC;

-- =============================================================================
-- Section 5: Window Equivalent of Section 1
-- =============================================================================

-- Replaces the scalar correlated subquery with AVG OVER () for efficiency.

SELECT
    makename,
    saleprice,
    ROUND(AVG(saleprice) OVER (), 2) AS grand_avg,
    saleprice - ROUND(AVG(saleprice) OVER (), 2) AS vs_grand_avg
FROM allsales
ORDER BY makename;

-- =============================================================================
-- Section 6: Sample Data
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
)

SELECT
    makename,
    saleprice,
    ROUND(AVG(saleprice) OVER (), 2) AS grand_avg,
    saleprice - ROUND(AVG(saleprice) OVER (), 2) AS vs_grand_avg,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (), 2) AS pct_of_total
FROM sample_data
ORDER BY makename;
