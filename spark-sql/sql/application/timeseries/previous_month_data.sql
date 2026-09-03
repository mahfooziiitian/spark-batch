-- Isolates stock purchased in the previous calendar month, summarised by make.
--
-- Date boundary technique (BETWEEN is inclusive on both ends):
--   Lower : TRUNC(CURDATE(), 'MONTH') - INTERVAL 1 MONTH
--             → first day of the previous month  (e.g. 2026-03-01)
--   Upper : LAST_DAY(CURDATE() - INTERVAL 1 MONTH)
--             → last  day of the previous month  (e.g. 2026-03-31)
--
-- Note: LAST_DAY(CURDATE()) - INTERVAL 1 MONTH is subtly wrong for months
--       with unequal lengths (Apr 30 → Mar 30, skipping Mar 31).
--       Always compute LAST_DAY *after* shifting the date, not before.

-- Sample data: make / model / stock across three months
WITH make AS (
    SELECT
        1 AS makeid,
        'Toyota' AS makename
    UNION ALL
    SELECT
        2 AS makeid,
        'Honda' AS makename
    UNION ALL
    SELECT
        3 AS makeid,
        'Ford' AS makename
),

model AS (
    SELECT
        101 AS modelid,
        1 AS makeid,
        'Camry' AS modelname
    UNION ALL
    SELECT
        102 AS modelid,
        1 AS makeid,
        'RAV4' AS modelname
    UNION ALL
    SELECT
        201 AS modelid,
        2 AS makeid,
        'Civic' AS modelname
    UNION ALL
    SELECT
        301 AS modelid,
        3 AS makeid,
        'F-150' AS modelname
),

stock AS (
    -- Previous month (March 2026) — included
    SELECT
        1 AS stockid,
        101 AS modelid,
        CAST('2026-03-05' AS DATE) AS datebought,
        28000.00 AS cost
    UNION ALL
    SELECT
        2 AS stockid,
        102 AS modelid,
        CAST('2026-03-12' AS DATE) AS datebought,
        34500.00 AS cost
    UNION ALL
    SELECT
        3 AS stockid,
        201 AS modelid,
        CAST('2026-03-18' AS DATE) AS datebought,
        22000.00 AS cost
    UNION ALL
    SELECT
        4 AS stockid,
        301 AS modelid,
        CAST('2026-03-25' AS DATE) AS datebought,
        41000.00 AS cost
    UNION ALL
    SELECT
        5 AS stockid,
        101 AS modelid,
        CAST('2026-03-31' AS DATE) AS datebought,
        29500.00 AS cost
    UNION ALL
    -- Current month (April 2026) — excluded
    SELECT
        6 AS stockid,
        102 AS modelid,
        CAST('2026-04-02' AS DATE) AS datebought,
        36000.00 AS cost
    UNION ALL
    SELECT
        7 AS stockid,
        201 AS modelid,
        CAST('2026-04-10' AS DATE) AS datebought,
        25000.00 AS cost
    UNION ALL
    -- Two months ago (February 2026) — excluded
    SELECT
        8 AS stockid,
        301 AS modelid,
        CAST('2026-02-14' AS DATE) AS datebought,
        39000.00 AS cost
)

SELECT
    mk.makename,
    SUM(st.cost) AS totalcost
FROM make AS mk
INNER JOIN model AS md
    ON mk.makeid = md.makeid
INNER JOIN stock AS st
    ON md.modelid = st.modelid
WHERE
    st.datebought BETWEEN
    TRUNC(CURDATE(), 'MONTH') - INTERVAL 1 MONTH
    AND LAST_DAY(CURDATE() - INTERVAL 1 MONTH)
GROUP BY mk.makename
ORDER BY mk.makename;

/* Expected output (run on 2026-04-12 — previous month = March 2026):
   makename | totalcost
   ---------+----------
   Ford     |  41000.00
   Honda    |  22000.00
   Toyota   |  92000.00   (28000 + 34500 + 29500)
*/
