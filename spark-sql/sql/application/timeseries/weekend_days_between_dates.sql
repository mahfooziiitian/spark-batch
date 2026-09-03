-- Counts weekend days (Saturday and Sunday) between two dates by expanding
-- a tally table CTE into a date series, then filtering for weekend days.
--
-- DAYOFWEEK convention in Spark SQL (same as MySQL):
--   1 = Sunday, 2 = Monday … 6 = Friday, 7 = Saturday
--   Weekend filter → IN (1, 7)
--
-- Two fixes applied to the original query:
--   1. DAYOFWEEK IN (5, 6) selects Thursday/Friday, not Saturday/Sunday.
--      Corrected to IN (1, 7).
--   2. Nm <= DATEDIFF(end, start) misses the last date because DATEDIFF
--      returns 60 for a 61-day range. Corrected to DATEDIFF(...) + 1.

-- ─────────────────────────────────────────────────────────────────────────────
-- Approach 1 — Tally table  (mirrors the original pattern, fully portable)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- TallyTable_CTE generates integers 1..90 via Spark's SEQUENCE function.
-- On systems without SEQUENCE, replace with:
--   SELECT ROW_NUMBER() OVER (ORDER BY any_col) AS nm FROM any_large_table LIMIT 90

WITH tally_cte AS (
    SELECT EXPLODE(SEQUENCE(1, 90)) AS nm
),

weekend_list_cte AS (
    SELECT DATE_ADD('2018-03-01', nm - 1) AS weekend_date
    FROM tally_cte
    WHERE
        DAYOFWEEK(DATE_ADD('2018-03-01', nm - 1)) IN (1, 7)
        AND nm <= DATEDIFF('2018-04-30', '2018-03-01') + 1
)

SELECT COUNT(*) AS weekenddays
FROM weekend_list_cte;

/* Expected output:
   weekenddays
   -----------
            18

   March 2018 : 5 × Saturday (3,10,17,24,31) + 4 × Sunday (4,11,18,25) =  9
   April 2018 : 4 × Saturday (7,14,21,28)    + 5 × Sunday (1,8,15,22,29) = 9
   Total      : 18
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- Approach 2 — Spark-native date SEQUENCE  (no tally table needed)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- SEQUENCE(start_date, end_date) generates every date in the range
-- (inclusive on both ends) as an array; EXPLODE turns it into rows.

SELECT COUNT(*) AS weekenddays
FROM (
    SELECT  -- noqa: ST05
        EXPLODE(SEQUENCE(
            TO_DATE('2018-03-01'),
            TO_DATE('2018-04-30')
        )) AS calendar_date
) AS calendar
WHERE DAYOFWEEK(calendar_date) IN (1, 7);
/* Expected output: same 18 weekend days */
