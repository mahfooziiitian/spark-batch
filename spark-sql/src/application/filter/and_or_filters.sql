-- Demonstrates AND, OR, NOT, IN, NOT IN, and complex boolean filter combinations.
-- Schema: allsales(makename, color, saleprice, saledate, customername)

-- =============================================================================
-- Section 1: OR — rows matching either colour condition
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE color = 'Red' OR color = 'Blue'
ORDER BY makename;

-- =============================================================================
-- Section 2: AND — Ferrari sales above a price threshold
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE makename = 'Ferrari' AND saleprice > 50000
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 3: Exclusion — Ferrari sales that are not Black
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE makename = 'Ferrari' AND color != 'Black'
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 4: Grouped logic — high-value Ferrari OR Bentley sales
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE (makename = 'Ferrari' OR makename = 'Bentley') AND saleprice > 80000
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 5: IN list — multiple makes in one filter
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE makename IN ('Ferrari', 'Bentley', 'Rolls Royce')
ORDER BY makename ASC, saleprice DESC;

-- =============================================================================
-- Section 6: NOT IN — exclude specific colours (note: fails if list has NULL)
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE color NOT IN ('Black', 'Silver')
ORDER BY makename;

-- =============================================================================
-- Section 7: Complex combination — two independent make+price pairs
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE
    (makename = 'Ferrari' AND saleprice > 70000)
    OR (makename = 'Bentley' AND saleprice > 85000)
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 8: Sample data
-- =============================================================================
-- Section 5 result (IN list):
-- makename     color   saleprice
-- Bentley      Black   92000.00
-- Bentley      Red     80000.00
-- Ferrari      Blue    72000.00
-- Ferrari      Red     65000.00
-- Rolls Royce  Silver  115000.00
