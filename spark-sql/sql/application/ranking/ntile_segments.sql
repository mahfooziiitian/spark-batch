-- Demonstrates NTILE() for dividing data into equal-sized segments:
-- deciles (10), quintiles (5), and quartiles (4).
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: NTILE(10) for deciles (1=lowest 10%, 10=top 10%)
-- =============================================================================
SELECT
    makename,
    saleprice,
    NTILE(10) OVER (ORDER BY saleprice ASC) AS decile
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 2: NTILE(5) for quintiles
-- =============================================================================
SELECT
    makename,
    saleprice,
    NTILE(5) OVER (ORDER BY saleprice ASC) AS quintile
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 3: NTILE(4) for quartiles
-- =============================================================================
SELECT
    makename,
    saleprice,
    NTILE(4) OVER (ORDER BY saleprice ASC) AS quartile
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 4: Extract data from specific quintile (quintile = 5 is top 20%)
-- =============================================================================
WITH quintile_ranked AS (
    SELECT
        makename,
        saleprice,
        NTILE(5) OVER (ORDER BY saleprice ASC) AS quintile
    FROM allsales
)

SELECT
    makename,
    saleprice,
    quintile
FROM quintile_ranked
WHERE quintile = 5
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 5: CASE on decile for descriptive labels
-- =============================================================================
WITH decile_ranked AS (
    SELECT
        makename,
        saleprice,
        NTILE(10) OVER (ORDER BY saleprice ASC) AS decile
    FROM allsales
)

SELECT
    makename,
    saleprice,
    decile,
    CASE decile
        WHEN 10 THEN 'Top 10%'
        WHEN 9 THEN 'Second 10%'
        WHEN 8 THEN 'Third 10%'
        WHEN 7 THEN 'Fourth 10%'
        WHEN 6 THEN 'Fifth 10%'
        WHEN 5 THEN 'Sixth 10%'
        WHEN 4 THEN 'Seventh 10%'
        WHEN 3 THEN 'Eighth 10%'
        WHEN 2 THEN 'Ninth 10%'
        ELSE 'Bottom 10%'
    END AS decile_label
FROM decile_ranked
ORDER BY saleprice;

-- =============================================================================
-- Section 6: Sample data with 10 rows showing clear decile assignment
-- =============================================================================
-- saleprice   decile  quintile  quartile
-- 55000.00    1       1         1
-- 65000.00    2       1         1
-- 68000.00    3       2         1
-- 72000.00    4       2         2
-- 78000.00    5       3         2
-- 80000.00    6       3         3
-- 85000.00    7       4         3
-- 90000.00    8       4         3
-- 95000.00    9       5         4
-- 115000.00   10      5         4
