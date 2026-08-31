-- Date and time type examples in Spark SQL (Databricks dialect).
-- Covers current date/time, literals, arithmetic, components, formatting,
-- truncation, intervals, UNIX timestamps, and timezone handling.

CREATE OR REPLACE TEMP VIEW events AS
SELECT *
FROM
    VALUES
    (1, TIMESTAMP '2024-03-15 08:45:00', DATE '2024-03-15'),
    (2, TIMESTAMP '2024-06-20 14:30:00', DATE '2024-06-20'),
    (3, TIMESTAMP '2024-11-01 23:59:59', DATE '2024-11-01'),
    (4, TIMESTAMP '2025-01-07 00:00:00', DATE '2025-01-07')
        AS events (id, event_ts, event_date);

---
-- 1. Current date and time
---

SELECT
    CURRENT_DATE() AS today,         -- Result: current date
    -- Result: current timestamp (session timezone)
    CURRENT_TIMESTAMP() AS now_ts,
    NOW() AS now_alias;     -- alias for CURRENT_TIMESTAMP()

---
-- 2. Date and timestamp literals
---

SELECT
    DATE '2024-01-15' AS date_literal,
    TIMESTAMP '2024-01-15 10:30:00' AS ts_literal,
    TIMESTAMP '2024-01-15 10:30:00.123' AS ts_millis;

---
-- 3. Date arithmetic
---

SELECT
    event_date,
    DATE_ADD(event_date, 7) AS plus_7_days,    -- Result: +7 days
    DATE_SUB(event_date, 30) AS minus_30_days,  -- Result: -30 days
    DATEDIFF(CURRENT_DATE(), event_date) AS days_ago,   -- days between dates
    ADD_MONTHS(event_date, 3) AS plus_3_months   -- add calendar months
FROM events;

---
-- 4. Extracting date components
---

SELECT
    event_date,
    YEAR(event_date) AS yr,
    MONTH(event_date) AS mo,
    DAY(event_date) AS dy,
    DAYOFWEEK(event_date) AS dow,        -- 1=Sunday … 7=Saturday
    DAYOFYEAR(event_date) AS doy,
    WEEKOFYEAR(event_date) AS woy,
    QUARTER(event_date) AS qtr
FROM events;

---
-- 5. Extracting time components
---

SELECT
    event_ts,
    HOUR(event_ts) AS hr,
    MINUTE(event_ts) AS min,
    SECOND(event_ts) AS sec
FROM events;

---
-- 6. Truncation
---

SELECT
    event_ts,
    -- Result: 2024-01-01 00:00:00
    DATE_TRUNC('YEAR', event_ts) AS trunc_year,
    -- Result: first day of month
    DATE_TRUNC('MONTH', event_ts) AS trunc_month,
    DATE_TRUNC('DAY', event_ts) AS trunc_day,     -- Result: midnight
    DATE_TRUNC('HOUR', event_ts) AS trunc_hour,    -- Result: top of the hour
    DATE_TRUNC('MINUTE', event_ts) AS trunc_minute,
    TRUNC(event_date, 'YEAR') AS trunc_yr_date, -- date-only truncation
    TRUNC(event_date, 'MONTH') AS trunc_mo_date
FROM events;

---
-- 7. Formatting and parsing
---

SELECT
    event_ts,
    DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS iso_date,
    DATE_FORMAT(event_ts, 'yyyy-MM-dd HH:mm:ss') AS iso_ts,
    DATE_FORMAT(event_date, 'dd/MM/yyyy') AS eu_date,
    DATE_FORMAT(event_ts, 'EEEE, MMMM d yyyy') AS long_form
FROM events;

-- Parsing strings into dates / timestamps
SELECT
    TO_DATE('2024-07-04', 'yyyy-MM-dd') AS parsed_date,
    TO_DATE('04/07/2024', 'dd/MM/yyyy') AS parsed_eu,
    TO_TIMESTAMP('2024-07-04 12:00', 'yyyy-MM-dd HH:mm') AS parsed_ts;

---
-- 8. Casting
---

SELECT
    CAST('2024-06-01' AS DATE) AS cast_date,
    CAST('2024-06-01 12:30:00' AS TIMESTAMP) AS cast_ts,
    CAST(event_ts AS DATE) AS ts_to_date,
    CAST(event_date AS TIMESTAMP) AS date_to_ts  -- midnight UTC
FROM events;

---
-- 9. Interval arithmetic
---

SELECT
    event_ts,
    event_ts + INTERVAL 1 DAY AS next_day,
    event_ts - INTERVAL 2 HOURS AS two_hrs_ago,
    event_ts + INTERVAL '1 3' DAY TO HOUR AS plus_1d_3h,   -- composite interval
    event_date + INTERVAL 1 MONTH AS next_month,
    event_date - INTERVAL 1 YEAR AS last_year
FROM events;

---
-- 10. UNIX timestamps
---

SELECT
    event_ts,
    UNIX_TIMESTAMP(event_ts) AS epoch_seconds,
    FROM_UNIXTIME(UNIX_TIMESTAMP(event_ts)) AS back_to_ts,
    FROM_UNIXTIME(0) AS unix_epoch_start  -- Result: 1970-01-01 00:00:00
FROM events;

-- Use UNIX epoch for bucket arithmetic (e.g., 15-minute buckets)
SELECT
    event_ts,
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_ts) / 900) * 900) AS bucket_15min
FROM events;

---
-- 11. Timezone handling
---

-- AT TIME ZONE — convert a timestamp to a different timezone
SELECT
    event_ts,
    event_ts AT TIME ZONE 'UTC' AS utc_ts,
    event_ts AT TIME ZONE 'America/New_York' AS ny_ts,
    event_ts AT TIME ZONE 'Europe/London' AS london_ts
FROM events;

-- TO_UTC_TIMESTAMP — interpret a local timestamp as a given zone, return UTC
SELECT TO_UTC_TIMESTAMP(
    TIMESTAMP '2024-06-01 12:00:00', 'America/Los_Angeles'
) AS la_to_utc;
-- Result: 2024-06-01 19:00:00 (PDT is UTC-7)

-- FROM_UTC_TIMESTAMP — convert from UTC to a local timezone
SELECT FROM_UTC_TIMESTAMP(
    TIMESTAMP '2024-06-01 19:00:00', 'America/Los_Angeles'
) AS utc_to_la;
-- Result: 2024-06-01 12:00:00
