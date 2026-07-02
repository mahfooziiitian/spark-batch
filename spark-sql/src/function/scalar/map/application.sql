-- ============================================================
-- Topic: Scalar functions — map applications
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Demonstrates patterns for rebuilding maps after key renaming.
-- ============================================================

-- Remove exact duplicates of the pair (key and value)
-- flat(id, new_key, value)  -- produced from map_entries + your rename
SELECT
    id,
    map_from_entries(
        collect_set(named_struct('key', new_key, 'value', value))
        -- or: array_distinct(collect_list(named_struct('key', new_key, 'value', value)))
    ) AS updated_map
FROM flat
GROUP BY id;

-- Make keys distinct (choose one value per key)
WITH ranked AS (
    SELECT
        id,
        new_key,
        value,
        row_number() OVER (
            PARTITION BY id, new_key
            ORDER BY event_ts /* set your precedence here */
            /* e.g., event_ts DESC, or CASE old_key WHEN 'a' THEN 2 WHEN 'b' THEN 1 ELSE 0 END DESC */
        ) AS rn
    FROM flat
)

SELECT
    id,
    map_from_entries(
        collect_list(named_struct('key', new_key, 'value', value))
    ) AS updated_map
FROM ranked
WHERE rn = 1
GROUP BY id;

-- Use collect_set(named_struct(...)) (or array_distinct) only if you need pair-wise distinct.
-- If you need key-wise distinct, rank or aggregate by new_key first, then map_from_entries.

-- Keep ONE value per new key (choose by priority / old key)
-- my_table(id, my_map)
-- rename_map: old_key -> new_key (literal or built from a table)
WITH cfg AS (
    SELECT map('a', 'alpha', 'b', 'alpha', 'c', 'gamma') AS rename_map
),

flat AS (
    SELECT
        t.id,
        e.k AS old_key,
        e.v AS value,
        coalesce(element_at(cfg.rename_map, e.k), e.k) AS new_key
    FROM my_table AS t
    CROSS JOIN cfg
    LATERAL VIEW inline(map_entries(t.my_map)) e AS k, v
),

picked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY id, new_key
            ORDER BY
                -- <<< set your precedence here >>>
                CASE old_key
                    WHEN 'a' THEN 2 -- 'a' beats 'b'
                    WHEN 'b' THEN 1
                    ELSE 0
                END DESC
        ) AS rn
    FROM flat
)

SELECT
    id,
    map_from_entries(
        collect_list(named_struct('key', new_key, 'value', value))
    ) AS updated_map
FROM picked
WHERE rn = 1
GROUP BY id;

-- Edit the CASE old_key … to encode who should win when keys collide.

-- Merge colliding values (e.g., SUM for numerics, ARRAY for strings)
-- Sum numeric values:
WITH cfg AS (
    SELECT map('a', 'alpha', 'b', 'alpha') AS rename_map
),

flat AS (
    SELECT
        t.id,
        e.v AS value,
        coalesce(element_at(cfg.rename_map, e.k), e.k) AS new_key
    FROM my_table AS t
    CROSS JOIN cfg
    LATERAL VIEW inline(map_entries(t.my_map)) e AS k, v
),

merged AS (
    SELECT
        id,
        new_key,
        sum(value) AS value
    FROM flat
    GROUP BY
        id,
        new_key
)

SELECT
    id,
    map_from_entries(
        collect_list(named_struct('key', new_key, 'value', value))
    ) AS updated_map
FROM merged
GROUP BY id;

-- Collect all into arrays:
-- WITH cfg AS (...),
-- flat AS (... same as above ...),
-- merged AS (
--     SELECT id, new_key, collect_list(value) AS value
--     FROM flat
--     GROUP BY id, new_key
-- )
-- SELECT id,
--     map_from_entries(
--         collect_list(named_struct('key', new_key, 'value', value))
--     ) AS updated_map
-- FROM merged
-- GROUP BY id;

-- Why not just map_from_entries(transform(...))?
-- Because maps are unordered, and if renames collide you must collapse duplicates first
-- via a window or GROUP BY or you will hit [DUPLICATED_MAP_KEY].
