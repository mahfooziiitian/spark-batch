-- Demonstrates NULL handling in filters: IS NULL, IS NOT NULL,
-- COALESCE in filters, null-safe equality, and NOT IN pitfalls.
-- Schema: allsales(makename, color, saleprice, cost, customername)

-- =============================================================================
-- Section 1: WHERE color IS NULL — find rows with no colour
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE color IS NULL
ORDER BY makename;

-- =============================================================================
-- Section 2: WHERE color IS NOT NULL
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE color IS NOT NULL
ORDER BY makename, color;

-- =============================================================================
-- Section 3: Multiple NULL checks — missing cost with known sale price
-- =============================================================================
SELECT
    makename,
    saleprice,
    cost
FROM allsales
WHERE cost IS NULL AND saleprice IS NOT NULL
ORDER BY makename;

-- =============================================================================
-- Section 4: COALESCE in filter — treat NULL as 'Unknown'
-- =============================================================================
SELECT
    makename,
    saleprice,
    COALESCE(color, 'Unknown') AS color_display
FROM allsales
WHERE COALESCE(color, 'Unknown') = 'Unknown'
ORDER BY makename;

-- =============================================================================
-- Section 5: Null-safe equality operator <=> (includes NULL <=> NULL as true)
-- =============================================================================
SELECT
    makename,
    color,
    saleprice
FROM allsales
WHERE color <=> 'Red'
ORDER BY makename;

-- =============================================================================
-- Section 6: NOT IN pitfall with NULLs — and the safe NOT EXISTS alternative
-- =============================================================================
-- Dangerous: returns no rows if subquery result contains any NULL value
-- WHERE makename NOT IN (SELECT makename FROM allsales WHERE makename IS NULL)

-- Safe alternative using NOT EXISTS:
SELECT
    a.makename,
    a.saleprice
FROM allsales AS a
WHERE NOT EXISTS (
    SELECT 1
    FROM allsales AS b
    WHERE b.makename = a.makename
        AND b.saleprice > 150000
)
ORDER BY a.makename;

-- =============================================================================
-- Section 7: Sample data with deliberate NULLs
-- =============================================================================
-- Sample: 8 rows, 2 have NULL color, 1 has NULL cost
-- Section 1 result (color IS NULL): 2 rows returned
-- Section 3 result (cost IS NULL AND saleprice IS NOT NULL): 1 row returned
-- Section 5 result (color <=> 'Red'): rows where color = 'Red' (NULL <=> 'Red' = false)
