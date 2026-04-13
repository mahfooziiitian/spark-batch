-- Controls where NULL values appear in ORDER BY results using NULLS FIRST and NULLS LAST.

-- =============================================================================
-- Section 1: Default NULL Ordering
-- =============================================================================

-- In Spark SQL: NULLs sort LAST in ASC order, FIRST in DESC order by default.

SELECT
    makename,
    color,
    saleprice
FROM allsales
ORDER BY color ASC;

-- =============================================================================
-- Section 2: NULLS FIRST — Force NULLs to Top
-- =============================================================================

SELECT
    makename,
    color,
    saleprice
FROM allsales
ORDER BY color ASC NULLS FIRST;

-- =============================================================================
-- Section 3: NULLS LAST — Force NULLs to Bottom
-- =============================================================================

SELECT
    makename,
    color,
    saleprice
FROM allsales
ORDER BY color DESC NULLS LAST;

-- =============================================================================
-- Section 4: Mixed Sort — NULL Control on One Column
-- =============================================================================

-- Sort by makename normally, but force NULLs in color to appear last.

SELECT
    makename,
    color,
    saleprice
FROM allsales
ORDER BY makename ASC, color ASC NULLS LAST;

-- =============================================================================
-- Section 5: COALESCE Workaround
-- =============================================================================

-- COALESCE(color, 'ZZZZ') pushes NULLs to the end without NULLS LAST syntax.

SELECT
    makename,
    color,
    saleprice
FROM allsales
ORDER BY COALESCE(color, 'ZZZZ') ASC;

-- =============================================================================
-- Section 6: CASE Sentinel for NULL Sort Position
-- =============================================================================

-- CASE WHEN gives precise control: NULLs get sort value 1 (after non-NULLs at 0).

SELECT
    makename,
    color,
    saleprice
FROM allsales
ORDER BY
    CASE WHEN color IS NULL THEN 1 ELSE 0 END,
    color ASC;

-- =============================================================================
-- Section 7: Sample Data with Deliberate NULL Colors
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        CAST(NULL AS STRING) AS color,
        65000.00 AS saleprice
    UNION ALL
    SELECT
        'Bentley' AS makename,
        'Black' AS color,
        90000.00 AS saleprice
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        CAST(NULL AS STRING) AS color,
        110000.00 AS saleprice
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        'Silver' AS color,
        155000.00 AS saleprice
    UNION ALL
    SELECT
        'Aston Martin' AS makename,
        'Green' AS color,
        75000.00 AS saleprice
)

SELECT
    makename,
    color,
    saleprice
FROM sample_data
ORDER BY
    CASE WHEN color IS NULL THEN 1 ELSE 0 END,
    color ASC;
