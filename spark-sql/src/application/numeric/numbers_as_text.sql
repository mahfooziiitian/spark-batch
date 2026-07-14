-- Handles source data where numeric values are stored as strings.

-- =============================================================================
-- Section 1: CAST String to Numeric
-- =============================================================================

-- Direct CAST from a clean string literal to a decimal type.

SELECT CAST('42500.00' AS DECIMAL(10, 2)) AS parsed_amount;

-- =============================================================================
-- Section 2: TRY_CAST for Safe Parsing
-- =============================================================================

-- TRY_CAST returns NULL instead of raising an error on invalid strings.

SELECT
    TRY_CAST('42500.00' AS DECIMAL(10, 2)) AS valid_amount,
    TRY_CAST('not_a_number' AS DECIMAL(10, 2)) AS bad_amount,
    TRY_CAST('' AS DECIMAL(10, 2)) AS empty_amount;

-- =============================================================================
-- Section 3: Remove Currency Symbols Before Casting
-- =============================================================================

-- REGEXP_REPLACE strips £, $, and comma characters before the CAST.

SELECT
    '£42,500.00' AS price_str,
    CAST(REGEXP_REPLACE('£42,500.00', '[£$,]', '') AS DECIMAL(10, 2)) AS cleaned_amount;

-- =============================================================================
-- Section 4: Filter Rows Where Cast Succeeds
-- =============================================================================

-- Use TRY_CAST IS NOT NULL to keep only parsable values.

WITH raw_data AS (
    SELECT '65000.00' AS price_col
    UNION ALL
    SELECT '90000.50' AS price_col
    UNION ALL
    SELECT 'N/A' AS price_col
    UNION ALL
    SELECT '120000' AS price_col
    UNION ALL
    SELECT 'TBD' AS price_col
)

SELECT
    price_col,
    TRY_CAST(price_col AS DOUBLE) AS parsed_price
FROM raw_data
WHERE TRY_CAST(price_col AS DOUBLE) IS NOT NULL
ORDER BY parsed_price;

-- =============================================================================
-- Section 5: CASE to Validate Before Arithmetic
-- =============================================================================

-- Use CASE + TRY_CAST to avoid arithmetic on unparsable rows.

WITH raw_data AS (
    SELECT '65000.00' AS price_col
    UNION ALL
    SELECT 'N/A' AS price_col
    UNION ALL
    SELECT '120000.00' AS price_col
    UNION ALL
    SELECT 'TBD' AS price_col
    UNION ALL
    SELECT '45000.50' AS price_col
)

SELECT
    price_col,
    CASE
        WHEN TRY_CAST(price_col AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(price_col AS DOUBLE) * 1.2
    END AS price_with_vat
FROM raw_data
ORDER BY price_col;

-- =============================================================================
-- Section 6: Sample Data — Mix of Valid and Invalid String Numbers
-- =============================================================================

WITH sample_data AS (
    SELECT
        '65000.00' AS price_str,
        'Ferrari' AS makename
    UNION ALL
    SELECT
        '£90,000.50' AS price_str,
        'Bentley' AS makename
    UNION ALL
    SELECT
        'TBD' AS price_str,
        'Lamborghini' AS makename
    UNION ALL
    SELECT
        '120000' AS price_str,
        'Rolls Royce' AS makename
    UNION ALL
    SELECT
        'N/A' AS price_str,
        'Aston Martin' AS makename
)

SELECT
    makename,
    price_str,
    TRY_CAST(price_str AS DOUBLE) AS try_raw,
    TRY_CAST(REGEXP_REPLACE(price_str, '[£$,]', '') AS DOUBLE) AS try_cleaned,
    CASE
        WHEN TRY_CAST(REGEXP_REPLACE(price_str, '[£$,]', '') AS DOUBLE) IS NOT NULL
            THEN TRY_CAST(REGEXP_REPLACE(price_str, '[£$,]', '') AS DOUBLE) * 1.2
    END AS price_with_vat
FROM sample_data
ORDER BY makename;
