-- Check if a user has premium features
WITH dataset AS (
    SELECT
        1 AS user_id,
        array('basic', 'chat', 'premium') AS features
    UNION ALL
    SELECT
        2 AS user_id,
        array('basic', 'chat') AS features
)

SELECT
    user_id,
    exists(features, f -> f = 'premium') AS has_premium
FROM dataset;

-- Detect abnormal sensor readings

WITH sensors AS (
    SELECT
        'd1' AS device,
        array(32.5, 33.0, 45.0) AS temps
    UNION ALL
    SELECT
        'd2' AS device,
        array(29.0, 30.0, 31.0) AS temps
)

SELECT
    device,
    exists(temps, t -> t > 40) AS has_overheat
FROM sensors;

-- Check if an order has a discounted item

WITH orders AS (
    SELECT
        101 AS order_id,
        array(0, 0, 20) AS discounts
    UNION ALL
    SELECT
        102 AS order_id,
        array(0, 0, 0) AS discounts
)

SELECT
    order_id,
    exists(discounts, d -> d > 0) AS has_discount
FROM orders;

-- Filter students who failed any subject

WITH students AS (
    SELECT
        'Alice' AS name,
        array(80, 90, 40) AS marks
    UNION ALL
    SELECT
        'Bob' AS name,
        array(70, 60, 65) AS marks
)

SELECT
    name,
    exists(marks, m -> m < 50) AS has_failed
FROM students;

-- Check if any tag matches a keyword

WITH logs AS (
    SELECT
        'req1' AS id,
        array('INFO', 'db', 'api') AS tags
    UNION ALL
    SELECT
        'req2' AS id,
        array('ERROR', 'auth') AS tags
)

SELECT
    id,
    exists(tags, t -> t = 'ERROR') AS has_error
FROM logs;

-- Check if any preference is enabled
WITH prefs AS (
    SELECT
        1 AS user_id,
        array(TRUE, FALSE, FALSE) AS flags
    UNION ALL
    SELECT
        2 AS user_id,
        array(FALSE, FALSE, FALSE) AS flags
)

SELECT
    user_id,
    exists(flags, f -> f = TRUE) AS any_enabled
FROM prefs;
