-- MAP type examples in Spark SQL (Databricks dialect).
-- Covers creation, access, key/value functions, HOFs, and aggregation patterns.

CREATE OR REPLACE TEMP VIEW sessions AS
SELECT *
FROM
    VALUES
    (1, 'Alice', MAP('page_views', 12, 'clicks', 3, 'purchases', 1)),
    (2, 'Bob', MAP('page_views', 5, 'clicks', 1, 'purchases', 0)),
    (3, 'Carol', MAP('page_views', 30, 'clicks', 8, 'purchases', 3)),
    (4, 'Dana', MAP('page_views', 7, 'clicks', 0, 'purchases', 0))
        AS sessions (id, name, metrics);

CREATE OR REPLACE TEMP VIEW inventory AS
SELECT *
FROM
    VALUES
    ('WH-A', MAP('laptop', 10, 'mouse', 50, 'keyboard', 30)),
    ('WH-B', MAP('laptop', 5, 'monitor', 20)),
    ('WH-C', MAP('mouse', 15, 'keyboard', 25, 'cable', 100))
        AS inventory (warehouse, stock);

---
-- 1. Creating maps
---

-- MAP() — alternating key, value pairs
SELECT MAP('k1', 1, 'k2', 2, 'k3', 3) AS simple_map;
-- Result: {k1 -> 1, k2 -> 2, k3 -> 3}

-- MAP_FROM_ARRAYS — separate key and value arrays
SELECT MAP_FROM_ARRAYS(ARRAY('a', 'b', 'c'), ARRAY(10, 20, 30)) AS from_arrays;
-- Result: {a -> 10, b -> 20, c -> 30}

-- MAP_FROM_ENTRIES — array of (key, value) structs
SELECT MAP_FROM_ENTRIES(ARRAY(STRUCT('x', 1), STRUCT('y', 2))) AS from_entries;
-- Result: {x -> 1, y -> 2}

---
-- 2. Accessing values
---

SELECT
    name,
    metrics['page_views'] AS page_views,    -- bracket notation
    -- ELEMENT_AT (returns NULL if missing)
    ELEMENT_AT(metrics, 'clicks') AS clicks
FROM sessions;

---
-- 3. MAP_KEYS, MAP_VALUES, MAP_ENTRIES
---

SELECT
    name,
    -- Result: ['page_views', 'clicks', 'purchases']
    MAP_KEYS(metrics) AS metric_names,
    MAP_VALUES(metrics) AS metric_values, -- Result: [12, 3, 1]
    MAP_ENTRIES(metrics) AS entries        -- Result: [{page_views, 12}, ...]
FROM sessions;

---
-- 4. MAP_CONTAINS_KEY
---

SELECT
    name,
    -- Result: true for all
    MAP_CONTAINS_KEY(metrics, 'purchases') AS has_purchases
FROM sessions;

-- Filter only sessions that have a 'purchases' key
SELECT
    name,
    metrics['purchases'] AS purchases
FROM sessions
WHERE
    MAP_CONTAINS_KEY(metrics, 'purchases')
    AND metrics['purchases'] > 0;
-- Result: Alice (1), Carol (3)

---
-- 5. MAP_CONCAT — merge two maps (right map wins on key collision)
---

SELECT
    MAP_CONCAT(
        MAP('a', 1, 'b', 2),
        MAP('b', 99, 'c', 3)
    ) AS merged;
-- Result: {a -> 1, b -> 99, c -> 3}

-- Merge per-warehouse stock across two warehouses
SELECT MAP_CONCAT(a.stock, b.stock) AS combined_stock
FROM inventory AS a
INNER JOIN inventory AS b
    ON a.warehouse = 'WH-A' AND b.warehouse = 'WH-B';

---
-- 6. MAP_ZIP_WITH — element-wise combination of two maps with same keys
---

SELECT
    MAP_ZIP_WITH(
        MAP('a', 1, 'b', 2),
        MAP('a', 10, 'b', 20),
        (k, v1, v2) -> v1 + v2
    ) AS summed;
-- Result: {a -> 11, b -> 22}

---
-- 7. Iterating map entries with LATERAL VIEW EXPLODE
---

SELECT
    name,
    metric_key,
    metric_value
FROM sessions
    LATERAL VIEW EXPLODE(metrics) AS metric_key, metric_value;

-- Aggregate: total clicks across all sessions
SELECT
    metric_key,
    SUM(metric_value) AS total
FROM sessions
    LATERAL VIEW EXPLODE(metrics) AS metric_key, metric_value
GROUP BY metric_key;

---
-- 8. MAP_FILTER (HOF) — keep only entries matching a predicate
---

-- Keep only metrics with a value > 5
SELECT
    name,
    MAP_FILTER(metrics, (k, v) -> v > 5) AS high_activity_metrics
FROM sessions;
-- Result: Carol -> {page_views -> 30, clicks -> 8}, Alice -> {page_views -> 12}

---
-- 9. TRANSFORM_KEYS / TRANSFORM_VALUES (HOFs)
---

-- Uppercase all metric keys
SELECT
    name,
    TRANSFORM_KEYS(metrics, (k, v) -> UPPER(k)) AS upper_keys
FROM sessions;

-- Multiply all metric values by 10 (scale)
SELECT
    name,
    TRANSFORM_VALUES(metrics, (k, v) -> v * 10) AS scaled_metrics
FROM sessions;

-- Normalise: divide each metric by page_views
SELECT
    name,
    TRANSFORM_VALUES(
        metrics,
        (k, v) -> ROUND(v / metrics['page_views'], 4)
    ) AS normalised
FROM sessions
WHERE metrics['page_views'] > 0;

---
-- 10. Build a map from GROUP BY using MAP_FROM_ENTRIES + COLLECT_LIST
---

CREATE OR REPLACE TEMP VIEW event_counts AS
SELECT *
FROM
    VALUES
    ('Alice', 'click', 5),
    ('Alice', 'view', 20),
    ('Alice', 'purchase', 2),
    ('Bob', 'click', 1),
    ('Bob', 'view', 8)
        AS event_counts (user_name, event_type, cnt);

-- Roll up per-user event counts into a single map column
SELECT
    user_name,
    MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(event_type, cnt))) AS event_map
FROM event_counts
GROUP BY user_name;
-- Result:
--   Alice -> {click -> 5, view -> 20, purchase -> 2}
--   Bob   -> {click -> 1, view -> 8}
