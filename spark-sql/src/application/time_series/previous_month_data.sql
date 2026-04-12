-- Isolates rows belonging to the previous calendar month.
-- Works at any point in time: the filter re-evaluates on each run so
-- "previous month" always means the month before the current date.
--
-- Technique: truncate today to the first of the current month,
--   then step back one month to get the first of the previous month.
--   The upper bound is the first of the current month (exclusive ≡ inclusive
--   of the last second via < boundary).

-- Sample data: daily order transactions spanning three months
WITH orders AS (
    SELECT
        1001 AS order_id,
        CAST('2026-02-03' AS DATE) AS order_date,
        'Alice' AS customer,
        450.00 AS amount
    UNION ALL
    SELECT
        1002 AS order_id,
        CAST('2026-02-14' AS DATE) AS order_date,
        'Bob' AS customer,
        320.00 AS amount
    UNION ALL
    SELECT
        1003 AS order_id,
        CAST('2026-02-28' AS DATE) AS order_date,
        'Alice' AS customer,
        780.00 AS amount
    UNION ALL
    SELECT
        1004 AS order_id,
        CAST('2026-03-01' AS DATE) AS order_date,
        'Carol' AS customer,
        210.00 AS amount
    UNION ALL
    SELECT
        1005 AS order_id,
        CAST('2026-03-07' AS DATE) AS order_date,
        'Bob' AS customer,
        540.00 AS amount
    UNION ALL
    SELECT
        1006 AS order_id,
        CAST('2026-03-15' AS DATE) AS order_date,
        'Alice' AS customer,
        930.00 AS amount
    UNION ALL
    SELECT
        1007 AS order_id,
        CAST('2026-03-22' AS DATE) AS order_date,
        'Carol' AS customer,
        125.00 AS amount
    UNION ALL
    SELECT
        1008 AS order_id,
        CAST('2026-03-31' AS DATE) AS order_date,
        'Bob' AS customer,
        670.00 AS amount
    UNION ALL
    -- Current month rows — excluded by the filter
    SELECT
        1009 AS order_id,
        CAST('2026-04-01' AS DATE) AS order_date,
        'Alice' AS customer,
        300.00 AS amount
    UNION ALL
    SELECT
        1010 AS order_id,
        CAST('2026-04-10' AS DATE) AS order_date,
        'Carol' AS customer,
        880.00 AS amount
)

SELECT
    order_id,
    order_date,
    customer,
    amount
FROM orders
WHERE
    order_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURDATE()), -1)
    AND order_date <  DATE_TRUNC('MONTH', CURDATE())
ORDER BY order_date;

/* Expected output (run on 2026-04-12 — previous month = March 2026):
   order_id | order_date | customer | amount
   ---------+------------+----------+-------
       1004 | 2026-03-01 | Carol    | 210.00
       1005 | 2026-03-07 | Bob      | 540.00
       1006 | 2026-03-15 | Alice    | 930.00
       1007 | 2026-03-22 | Carol    | 125.00
       1008 | 2026-03-31 | Bob      | 670.00
*/
