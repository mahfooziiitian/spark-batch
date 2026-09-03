-- String formatting functions for shortening text, adding ellipses, and standardizing output.

-- =============================================================================
-- Section 1: Shortening Text with LEFT
-- =============================================================================

-- LEFT(string, n) returns the first n characters.

SELECT
    customername,
    LEFT(customername, 20) AS short_name
FROM customer
ORDER BY customername;

-- =============================================================================
-- Section 2: Adding Ellipses to Indicate Truncation
-- =============================================================================

SELECT
    customername,
    CASE
        WHEN LENGTH(customername) > 20
            THEN CONCAT(LEFT(customername, 17), '...')
        ELSE customername
    END AS display_name
FROM customer
ORDER BY customername;

-- =============================================================================
-- Section 3: SUBSTRING for Mid-String Extraction
-- =============================================================================

-- SUBSTRING(string, start, length) extracts a portion from any position.

SELECT
    customername,
    SUBSTRING(customername, 1, 10) AS first_10_chars,
    SUBSTRING(customername, 5, 8) AS mid_extract
FROM customer
ORDER BY customername;

-- =============================================================================
-- Section 4: RIGHT for Suffix Extraction
-- =============================================================================

-- RIGHT(string, n) returns the last n characters.

SELECT
    customername,
    RIGHT(customername, 5) AS last_5_chars
FROM customer
ORDER BY customername;

-- =============================================================================
-- Section 5: Padding with LPAD and RPAD
-- =============================================================================

-- LPAD pads from the left; RPAD pads from the right.

SELECT
    salesid,
    LPAD(CAST(salesid AS STRING), 6, '0') AS padded_id,
    RPAD(CAST(salesid AS STRING), 8, '-') AS rpad_id
FROM sales
ORDER BY salesid;

-- =============================================================================
-- Section 6: INITCAP — Capitalize First Letter of Each Word
-- =============================================================================

SELECT
    makename,
    INITCAP(LOWER(makename)) AS title_case
FROM allsales
ORDER BY makename;

-- =============================================================================
-- Section 7: TRIM, LTRIM, RTRIM for Whitespace Removal
-- =============================================================================

SELECT
    '  hello world  ' AS raw_string,
    TRIM('  hello world  ') AS trimmed,
    LTRIM('  hello world  ') AS ltrimmed,
    RTRIM('  hello world  ') AS rtrimmed;

-- =============================================================================
-- Section 8: REPEAT and SPACE Functions
-- =============================================================================

-- REPEAT repeats a string n times; SPACE(n) returns n space characters.

SELECT
    REPEAT('*', 5) AS stars,
    REPEAT('ha', 3) AS laugh,
    CONCAT('|', SPACE(10), '|') AS spaced;

-- =============================================================================
-- Section 9: Sample Data — Before/After Formatting
-- =============================================================================

WITH sample_data AS (
    SELECT 'Mr. Alexander Hamilton' AS customername
    UNION ALL
    SELECT 'VICTORIA QUEENSBURY-SMYTHE' AS customername
    UNION ALL
    SELECT 'Bob' AS customername
    UNION ALL
    SELECT '  padded name  ' AS customername
    UNION ALL
    SELECT 'A Very Long Customer Name That Exceeds Twenty Characters' AS customername
)

SELECT
    customername AS original,
    LEFT(customername, 20) AS left_20,
    CASE
        WHEN LENGTH(customername) > 20
            THEN CONCAT(LEFT(customername, 17), '...')
        ELSE customername
    END AS display_name,
    INITCAP(LOWER(TRIM(customername))) AS normalized,
    LPAD(CAST(LENGTH(customername) AS STRING), 4, '0') AS padded_length
FROM sample_data
ORDER BY customername;
