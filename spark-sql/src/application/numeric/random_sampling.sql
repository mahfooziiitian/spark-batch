-- Demonstrates TABLESAMPLE and ORDER BY RAND() for random dataset sampling.

-- =============================================================================
-- Section 1: TABLESAMPLE Percent
-- =============================================================================

-- Returns approximately 10% of rows; non-deterministic across runs.

SELECT
    makename,
    saleprice,
    saledate
FROM allsales TABLESAMPLE (10 PERCENT)
ORDER BY saledate;

-- =============================================================================
-- Section 2: TABLESAMPLE Rows
-- =============================================================================

-- Returns exactly 5 rows selected at random from the table.

SELECT
    makename,
    saleprice,
    saledate
FROM allsales TABLESAMPLE (5 ROWS)
ORDER BY saledate;

-- =============================================================================
-- Section 3: TABLESAMPLE with REPEATABLE Seed
-- =============================================================================

-- REPEATABLE(seed) makes the sample deterministic across runs.

SELECT
    makename,
    saleprice,
    saledate
FROM allsales TABLESAMPLE (10 PERCENT) REPEATABLE (42) -- noqa: AL01, AL05, CP02
ORDER BY saledate;

-- =============================================================================
-- Section 4: ORDER BY RAND() LIMIT n
-- =============================================================================

-- True shuffle-based random sample — guarantees exact count but is slower.

SELECT
    makename,
    saleprice,
    saledate
FROM allsales
ORDER BY RAND()
LIMIT 10;

-- =============================================================================
-- Section 5: RAND(seed) for Reproducible ORDER BY
-- =============================================================================

-- RAND(seed) produces the same shuffle each run when seed is fixed.

SELECT
    makename,
    saleprice,
    saledate
FROM allsales
ORDER BY RAND(99)
LIMIT 5;

-- =============================================================================
-- Section 6: Random Sample Per Group
-- =============================================================================

-- ROW_NUMBER() with RAND() ordering selects up to 2 random rows per make.

WITH ranked AS (
    SELECT
        makename,
        saleprice,
        saledate,
        ROW_NUMBER() OVER (PARTITION BY makename ORDER BY RAND()) AS rn
    FROM allsales
)

SELECT
    makename,
    saleprice,
    saledate
FROM ranked
WHERE rn <= 2
ORDER BY makename;
