-- Demonstrates LIKE patterns, NOT LIKE, case-insensitive wildcards,
-- and specific text position matching.
-- Schema: allsales(makename, modelname, customername, saleprice, saledate)

-- =============================================================================
-- Section 1: Starts with — makes beginning with F
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename LIKE 'F%'
ORDER BY makename;

-- =============================================================================
-- Section 2: Ends with — customers ending with Ltd
-- =============================================================================
SELECT
    customername,
    makename,
    saleprice
FROM allsales
WHERE customername LIKE '%Ltd'
ORDER BY customername;

-- =============================================================================
-- Section 3: Contains — makes containing 'ar'
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename LIKE '%ar%'
ORDER BY makename;

-- =============================================================================
-- Section 4: Single-char wildcard — model names matching DB_ pattern
-- =============================================================================
SELECT
    makename,
    modelname,
    saleprice
FROM allsales
WHERE modelname LIKE 'DB_'
ORDER BY modelname;

-- =============================================================================
-- Section 5: NOT LIKE exclusion — excludes makes starting with A
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename NOT LIKE 'A%'
ORDER BY makename;

-- =============================================================================
-- Section 6: Case-insensitive wildcard using LOWER()
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE LOWER(makename) LIKE '%ferrari%'
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 7: Alternative patterns — makes starting with F or B
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename LIKE 'F%' OR makename LIKE 'B%'
ORDER BY makename;

-- =============================================================================
-- Section 8: Specific text position matching
-- =============================================================================
SELECT
    customername,
    saleprice
FROM allsales
WHERE SUBSTRING(customername, 1, 3) = 'Top' OR LEFT(customername, 3) = 'Top'
ORDER BY customername;

-- =============================================================================
-- Section 9: Sample data
-- =============================================================================
-- Section 1 result (starts with F):
-- makename  saleprice
-- Ferrari   65000.00
-- Ferrari   72000.00
--
-- Section 7 result (F or B):
-- makename  saleprice
-- Bentley   80000.00
-- Bentley   90000.00
-- Ferrari   65000.00
-- Ferrari   72000.00
