-- Demonstrates RLIKE, REGEXP_LIKE, and REGEXP_EXTRACT for pattern-based
-- filtering and capture group extraction.
-- Schema: allsales(makename, customername, saleprice, saledate)

-- =============================================================================
-- Section 1: RLIKE starts-with character class — A, B, or F
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename RLIKE '^[ABF]'
ORDER BY makename;

-- =============================================================================
-- Section 2: RLIKE ends-with — customer names ending in Ltd
-- =============================================================================
SELECT
    customername,
    makename,
    saleprice
FROM allsales
WHERE customername RLIKE 'Ltd$'
ORDER BY customername;

-- =============================================================================
-- Section 3: RLIKE character range — makes starting with A through F
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename RLIKE '^[A-F]'
ORDER BY makename;

-- =============================================================================
-- Section 4: REGEXP_LIKE — case-insensitive match using (?i) flag
-- =============================================================================
SELECT
    customername,
    makename,
    saleprice
FROM allsales
WHERE REGEXP_LIKE(customername, '(?i)ltd')
ORDER BY customername;

-- =============================================================================
-- Section 5: Word boundary — exact word match using \b
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename RLIKE '\\bRolls\\b'
ORDER BY makename;

-- =============================================================================
-- Section 6: REGEXP_EXTRACT — capture company type suffix
-- =============================================================================
SELECT
    customername,
    makename,
    saleprice,
    REGEXP_EXTRACT(customername, '(Ltd|Plc|Inc)', 1) AS company_type
FROM allsales
ORDER BY company_type, customername;

-- =============================================================================
-- Section 7: NOT RLIKE — exclude makes starting with A or B
-- =============================================================================
SELECT
    makename,
    saleprice
FROM allsales
WHERE makename NOT RLIKE '^(A|B)'
ORDER BY makename;

-- =============================================================================
-- Section 8: Sample data
-- =============================================================================
-- Section 1 result (starts with A, B, or F):
-- makename      saleprice
-- Aston Martin  55000.00
-- Bentley       80000.00
-- Bentley       90000.00
-- Ferrari       65000.00
-- Ferrari       72000.00
--
-- Section 6 result (company_type extraction):
-- customername      company_type  makename  saleprice
-- Fast Cars Ltd     Ltd           Ferrari   65000.00
-- Grand Prix Inc    Inc           Ferrari   125000.00
-- Top Speed Ltd     Ltd           Ferrari   72000.00
