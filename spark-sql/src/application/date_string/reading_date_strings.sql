-- Converts date and datetime strings stored as text into proper Spark SQL
-- DATE / TIMESTAMP types using TO_DATE() and TO_TIMESTAMP().
--
-- Why this matters:
--   Spark loads unrecognised date formats as STRING. Once stored as a string,
--   date arithmetic (DATEDIFF, DATE_ADD, YEAR, …) is impossible until the
--   value is explicitly converted.
--
-- Format pattern reference (Java SimpleDateFormat tokens):
--   yyyy  = 4-digit year           MM   = month (01–12)
--   dd    = day of month (01–31)   MMM  = abbreviated month (Jan … Dec)
--   MMMM  = full month name        HH   = hour 24h (00–23)
--   hh    = hour 12h (01–12)       mm   = minutes (00–59)
--   ss    = seconds (00–59)        SSS  = milliseconds
--   a     = AM / PM marker         'T'  = literal character T

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 1: TO_DATE — common regional and ISO formats
-- ─────────────────────────────────────────────────────────────────────────────

-- Original example from the chapter
SELECT TO_DATE('12/31/2024', 'MM/dd/yyyy') AS new_years;

-- One query showing all common string formats converted to DATE
SELECT
    TO_DATE('12/31/2024', 'MM/dd/yyyy') AS us_slash,
    TO_DATE('31/12/2024', 'dd/MM/yyyy') AS uk_slash,
    TO_DATE('2024-12-31', 'yyyy-MM-dd') AS iso_8601,
    TO_DATE('31.12.2024', 'dd.MM.yyyy') AS european_dot,
    TO_DATE('31-Dec-2024', 'dd-MMM-yyyy') AS abbreviated_month,
    TO_DATE('December 31, 2024', 'MMMM dd, yyyy') AS full_month_name,
    TO_DATE('20241231', 'yyyyMMdd') AS compact_no_separator;

/* All seven expressions return the same DATE value: 2024-12-31 */

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 2: TO_TIMESTAMP — string to TIMESTAMP (date + time)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    TO_TIMESTAMP('12/31/2024 23:59:59', 'MM/dd/yyyy HH:mm:ss') AS us_datetime,
    TO_TIMESTAMP('2024-12-31T23:59:59', "yyyy-MM-dd'T'HH:mm:ss") AS iso_8601_datetime,
    TO_TIMESTAMP('31-Dec-2024 11:59:59 PM', 'dd-MMM-yyyy hh:mm:ss a') AS twelve_hour_ampm,
    TO_TIMESTAMP('20241231235959', 'yyyyMMddHHmmss') AS compact_datetime;

/* All four expressions return the same TIMESTAMP: 2024-12-31 23:59:59 */

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 3: Practical — convert a raw-data table with mixed string formats
-- ─────────────────────────────────────────────────────────────────────────────

WITH raw_sales AS (
    -- Simulates source data arriving in different regional formats
    SELECT
        1 AS id,
        'Alice' AS salesperson,
        '12/31/2024' AS raw_date,
        'MM/dd/yyyy' AS fmt,
        42000.00 AS saleprice
    UNION ALL
    SELECT
        2 AS id,
        'Bob' AS salesperson,
        '31/12/2024' AS raw_date,
        'dd/MM/yyyy' AS fmt,
        28500.00 AS saleprice
    UNION ALL
    SELECT
        3 AS id,
        'Carol' AS salesperson,
        '2024-12-31' AS raw_date,
        'yyyy-MM-dd' AS fmt,
        51000.00 AS saleprice
    UNION ALL
    SELECT
        4 AS id,
        'Dave' AS salesperson,
        '31-Dec-2024' AS raw_date,
        'dd-MMM-yyyy' AS fmt,
        19800.00 AS saleprice
    UNION ALL
    SELECT
        5 AS id,
        'Eve' AS salesperson,
        '20241231' AS raw_date,
        'yyyyMMdd' AS fmt,
        33200.00 AS saleprice
)

SELECT
    id,
    salesperson,
    raw_date,
    -- Each row's format is known ahead of time; use CASE to dispatch
    saleprice,
    CASE fmt
        WHEN 'MM/dd/yyyy' THEN TO_DATE(raw_date, 'MM/dd/yyyy')
        WHEN 'dd/MM/yyyy' THEN TO_DATE(raw_date, 'dd/MM/yyyy')
        WHEN 'yyyy-MM-dd' THEN TO_DATE(raw_date, 'yyyy-MM-dd')
        WHEN 'dd-MMM-yyyy' THEN TO_DATE(raw_date, 'dd-MMM-yyyy')
        WHEN 'yyyyMMdd' THEN TO_DATE(raw_date, 'yyyyMMdd')
    END AS sale_date
FROM raw_sales
ORDER BY id;

/* Expected output:
   id | salesperson | raw_date    | sale_date  | saleprice
   ---+-------------+-------------+------------+----------
    1 | Alice       | 12/31/2024  | 2024-12-31 |  42000.00
    2 | Bob         | 31/12/2024  | 2024-12-31 |  28500.00
    3 | Carol       | 2024-12-31  | 2024-12-31 |  51000.00
    4 | Dave        | 31-Dec-2024 | 2024-12-31 |  19800.00
    5 | Eve         | 20241231    | 2024-12-31 |  33200.00
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 4: TRY_TO_DATE — safe parsing (returns NULL instead of error)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- When source data may contain unparseable values, TRY_TO_DATE silently
-- returns NULL rather than raising an AnalysisException.

SELECT
    TRY_TO_DATE('12/31/2024', 'MM/dd/yyyy') AS valid_date,
    TRY_TO_DATE('not-a-date', 'MM/dd/yyyy') AS invalid_returns_null,
    TRY_TO_DATE('31/12/2024', 'MM/dd/yyyy') AS wrong_format_returns_null;

/* Expected output:
   valid_date  | invalid_returns_null | wrong_format_returns_null
   ------------+----------------------+--------------------------
   2024-12-31  | NULL                 | NULL
*/
