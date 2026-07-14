-- Demonstrates combining text and numeric filters, case-insensitive matching,
-- and function-based filters on string columns.
-- Schema: allsales(makename, color, saleprice, saledate, customername)

-- =============================================================================
-- Section 1: Text filter combined with numeric threshold
-- =============================================================================
SELECT
    makename,
    saleprice,
    customername
FROM allsales
WHERE LOWER(makename) = 'ferrari' AND saleprice > 70000
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 2: LENGTH in filter — long customer names with low sale price
-- =============================================================================
SELECT
    customername,
    makename,
    saleprice
FROM allsales
WHERE LENGTH(customername) > 10 AND saleprice < 100000
ORDER BY LENGTH(customername) DESC;

-- =============================================================================
-- Section 3: UPPER case normalize filter
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE UPPER(color) = 'RED'
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 4: Case-insensitive LIKE using LOWER()
-- =============================================================================
SELECT
    customername,
    makename,
    saleprice
FROM allsales
WHERE LOWER(customername) LIKE '%ltd%'
ORDER BY customername;

-- =============================================================================
-- Section 5: Mixed date year and make name pattern
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice
FROM allsales
WHERE YEAR(saledate) = 2017 AND LOWER(makename) LIKE 'f%'
ORDER BY saledate;

-- =============================================================================
-- Section 6: Sample data
-- =============================================================================
-- Section 1 result:
-- makename  saleprice  customername
-- Ferrari   72000.00   Top Speed Ltd
-- Ferrari   125000.00  Grand Prix Inc
--
-- Section 4 result:
-- customername      makename  saleprice
-- Fast Cars Ltd     Ferrari   65000.00
-- Top Speed Ltd     Ferrari   72000.00
