-- Demonstrates correlated subqueries where the inner query references the outer query's row.

-- =============================================================================
-- Section 1: Simple Correlated — Customer Total Spend Per Sale
-- =============================================================================

-- For each sale, the inner query sums all sales for that same customer.

SELECT
    s.makename,
    s.saleprice,
    s.customername,
    (
        SELECT SUM(s2.saleprice)
        FROM allsales AS s2
        WHERE s2.customername = s.customername
    ) AS customer_total
FROM allsales AS s
ORDER BY s.customername, s.saledate;

-- =============================================================================
-- Section 2: Percentage of Customer's Total
-- =============================================================================

-- Divide each sale by the customer's total to get percentage contribution.

SELECT
    s.customername,
    s.makename,
    s.saleprice,
    ROUND(
        s.saleprice * 100.0
        / (
            SELECT SUM(s2.saleprice)
            FROM allsales AS s2
            WHERE s2.customername = s.customername
        ),
        2
    ) AS pct_of_customer_total
FROM allsales AS s
ORDER BY s.customername;

-- =============================================================================
-- Section 3: Mark Sales Above Make's Average
-- =============================================================================

-- Compare each sale to the average for its make using a correlated subquery.

SELECT
    s.makename,
    s.saleprice,
    CASE
        WHEN s.saleprice > (
            SELECT AVG(s2.saleprice)
            FROM allsales AS s2
            WHERE s2.makename = s.makename
        ) THEN 'Above make average'
        ELSE 'At or below make average'
    END AS vs_make_avg
FROM allsales AS s
ORDER BY s.makename, s.saleprice;

-- =============================================================================
-- Section 4: Window Function Equivalent (More Efficient)
-- =============================================================================

-- Replace correlated subquery with SUM OVER PARTITION BY for better performance.

SELECT
    makename,
    saleprice,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (PARTITION BY makename), 2) AS pct_of_make
FROM allsales
ORDER BY makename, saleprice;

-- =============================================================================
-- Section 5: Sample Data Demo
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        'Alice' AS customername
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        75000.00 AS saleprice,
        'Alice' AS customername
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        'Bob' AS customername
    UNION ALL
    SELECT
        'Bentley' AS makename,
        95000.00 AS saleprice,
        'Alice' AS customername
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        110000.00 AS saleprice,
        'Bob' AS customername
)

SELECT
    makename,
    customername,
    saleprice,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (PARTITION BY customername), 2) AS pct_of_customer_total,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (PARTITION BY makename), 2) AS pct_of_make_total
FROM sample_data
ORDER BY customername, makename;
