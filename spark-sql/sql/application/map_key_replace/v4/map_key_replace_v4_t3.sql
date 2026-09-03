-- Step-by-Step Strategy
-- Explode the map into key-value pairs
-- Apply the key replacement logic
-- Group by the new key and take the last value
-- Reconstruct the map

-- Case: 1 One key with one key in source table
-- Spark/Databricks SQL
SELECT map_from_entries(collect_list(struct(old_tag_key, new_tag_key))) AS key_map
FROM
    mgmt_stg.metadata.old_custom_tags_key_mapping;

WITH map_table (test_id, key_map, original_map) AS (
    -- 1) Simple remap, no collisions
    SELECT
        1,
        map('A', 'X', 'B', 'Y') AS key_map,
        map('A', '1', 'B', '2', 'C', '3') AS original_map
    UNION ALL
    -- 2) Collision with existing original key ('A' -> 'X' 
    -- but 'X' exists in original)
    SELECT
        2,
        map('A', 'X') AS key_map,
        map('A', '1', 'X', '9') AS original_map
    UNION ALL
    -- 3) Two different keys map to the same target ('A' -> 'Z', 'B' -> 'Z')
    SELECT
        3,
        map('A', 'Z', 'B', 'Z') AS key_map,
        map('A', '1', 'B', '2') AS original_map
    UNION ALL
    -- 4) Identity mapping (no-op), plus one remap
    SELECT
        4,
        map('A', 'A', 'B', 'B', 'C', 'K') AS key_map,
        map('A', '10', 'B', '20', 'C', '30') AS original_map
    UNION ALL
    -- 5) key_map contains mapping for a key not present in 
    -- original_map (ignored)
    SELECT
        5,
        map('Z', 'Q', 'C', 'K') AS key_map,
        map('A', '1', 'B', '2') AS original_map
    UNION ALL
    -- 6) Empty key_map
    SELECT
        6,
        map() AS key_map,
        map('A', 'foo', 'B', 'bar') AS original_map
    UNION ALL
    -- 7) Empty original_map
    SELECT
        7,
        map('A', 'X') AS key_map,
        map() AS original_map
    UNION ALL
    -- 8) Whitespace normalization: leading/trailing spaces in keys
    SELECT
        8,
        map(' Tenant', 'Tenant', 'Tent', 'Tenant', 'Environ', 'Env') AS key_map,
        map(' Tenant', '1', 'c', '3', 'Environ', 'prod') AS original_map
    UNION ALL
    -- 9) Case sensitivity: remap depends on exact case
    SELECT
        9,
        map('env', 'ENV', 'Prod', 'PROD') AS key_map,
        map('Env', '1', 'env', '2', 'Prod', '3') AS original_map
    UNION ALL
    -- 10) Null value allowed; null key is not allowed
    SELECT
        10,
        map('A', 'X') AS key_map,
        map('A', cast(NULL AS string), 'B', 'bbb') AS original_map
    UNION ALL
    -- 11) Mixed: one remap collides with original; another two 
    -- remap to same new key
    SELECT
        11,
        map('U', 'X', 'A', 'Z', 'B', 'Z') AS key_map,
        map(
            'U', '100', 'X', '999', 'A', '10', 'B', '20', 'C', '30'
        ) AS original_map
    UNION ALL
    -- 12) Single-hop remap only (no chaining): 'A'->'B' and 'B'->'C'
    SELECT
        12,
        map('A', 'B', 'B', 'C') AS key_map,
        map('A', '1', 'B', '2', 'C', '3') AS original_map
)

SELECT
    mt.test_id,
    mt.key_map,
    mt.original_map,
    map_from_entries(
        aggregate(
            aggregate(
                transform(
          map_entries(mt.original_map),--noqa
          kv -> struct(--noqa
            coalesce(mt.key_map[kv.key], kv.key) as new_key,  --noqa
            kv.key as original_key,--noqa
            kv.value as value--noqa
                    )
                ),
                cast(array() AS ARRAY<STRUCT<new_key: string, value: string>>),
        (acc, kv) -> if(--noqa
                    array_contains(map_keys(mt.original_map), kv.new_key)
                    AND kv.original_key != kv.new_key,
          acc, --noqa
          acc || array(struct(kv.new_key, kv.value))--noqa
                )
            ),
            cast(array() AS ARRAY<STRUCT<new_key: string, value: string>>),
      (acc, kv) -> filter(--noqa
        acc || array(kv),--noqa
        x -> x.new_key != kv.new_key OR x == kv  --noqa
            )
        )
    ) AS final_map
FROM map_table AS mt
ORDER BY mt.test_id;


-- =========Analysis================
WITH map_table (test_id, key_map, original_map) AS (
    -- 1) Simple remap, no collisions
    SELECT
        1,
        map('A', 'X', 'B', 'Y') AS key_map,
        map('A', '1', 'B', '2', 'C', '3') AS original_map
    UNION ALL
    -- 2) Collision with existing original key ('A' -> 'X' 
    -- but 'X' exists in original)
    SELECT
        2,
        map('A', 'X') AS key_map,
        map('A', '1', 'X', '9') AS original_map
    UNION ALL
    -- 3) Two different keys map to the same target ('A' -> 'Z', 'B' -> 'Z')
    SELECT
        3,
        map('A', 'Z', 'B', 'Z') AS key_map,
        map('A', '1', 'B', '2') AS original_map
    UNION ALL
    -- 4) Identity mapping (no-op), plus one remap
    SELECT
        4,
        map('A', 'A', 'B', 'B', 'C', 'K') AS key_map,
        map('A', '10', 'B', '20', 'C', '30') AS original_map
    UNION ALL
    -- 5) key_map contains mapping for a key not present 
    -- in original_map (ignored)
    SELECT
        5,
        map('Z', 'Q', 'C', 'K') AS key_map,
        map('A', '1', 'B', '2') AS original_map
    UNION ALL
    -- 6) Empty key_map
    SELECT
        6,
        map() AS key_map,
        map('A', 'foo', 'B', 'bar') AS original_map
    UNION ALL
    -- 7) Empty original_map
    SELECT
        7,
        map('A', 'X') AS key_map,
        map() AS original_map
    UNION ALL
    -- 8) Whitespace normalization: leading/trailing spaces in keys
    SELECT
        8,
        map(' Tenant', 'Tenant', 'Tent', 'Tenant', 'Environ', 'Env') AS key_map,
        map(' Tenant', '1', 'c', '3', 'Environ', 'prod') AS original_map
    UNION ALL
    -- 9) Case sensitivity: remap depends on exact case
    SELECT
        9,
        map('env', 'ENV', 'Prod', 'PROD') AS key_map,
        map('Env', '1', 'env', '2', 'Prod', '3') AS original_map
    UNION ALL
    -- 10) Null value allowed; null key is not allowed
    SELECT
        10,
        map('A', 'X') AS key_map,
        map('A', cast(NULL AS string), 'B', 'bbb') AS original_map
    UNION ALL
    -- 11) Mixed: one remap collides with original; 
    -- another two remap to same new key
    SELECT
        11,
        map('U', 'X', 'A', 'Z', 'B', 'Z') AS key_map,
        map(
            'U', '100', 'X', '999', 'A', '10', 'B', '20', 'C', '30'
        ) AS original_map
    UNION ALL
    -- 12) Single-hop remap only (no chaining): 'A'->'B' and 'B'->'C'
    SELECT
        12,
        map('A', 'B', 'B', 'C') AS key_map,
        map('A', '1', 'B', '2', 'C', '3') AS original_map
),

transformed_mapped_table (
    SELECT
        mt.key_map,
        mt.original_map,
        transform( --noqa
            map_entries(mt.original_map),
            kv -> struct(--noqa
                coalesce(
                    mt.key_map[kv.key], kv.key
            ) as new_key,--noqa
            kv.key as original_key,--noqa
            kv.value as value--noqa
            )
        ) AS transformed_map_entries
    FROM map_table AS mt
),

original_key_from_mapped_table (
    SELECT
        mt.key_map,
        mt.original_map,
        mt.transformed_map_entries,
        aggregate(
            mt.transformed_map_entries,
            cast(
                array() AS ARRAY<STRUCT<new_key: string, value: string>>
            ),
            (acc, kv) -> if( --noqa
                array_contains(map_keys(mt.original_map), kv.new_key) AND kv.original_key != kv.new_key,--noqa
                acc,--noqa
                acc || array(struct(kv.new_key, kv.value))--noqa
            )
        ) AS key_from_original
    FROM
        transformed_mapped_table AS mt
),

remove_dup_key_from_mapped_table (
    SELECT
        mt.key_map,
        mt.original_map,
        mt.transformed_map_entries,
        mt.key_from_original,
        aggregate(
            mt.key_from_original,-- noqa
            cast(
                array() AS ARRAY<STRUCT<new_key: string, value: string>>
            ),
            (acc, kv) -> filter(  -- noqa
                acc || array(kv),-- noqa
                x -> x.new_key != kv.new_key OR x == kv-- noqa
            )
        ) AS final_entries
    FROM
        original_key_from_mapped_table AS mt
)

SELECT
    mt.key_map,
    mt.original_map,
    mt.transformed_map_entries,
    mt.key_from_original,
    mt.final_entries,
    map_from_entries(mt.final_entries) AS final_map
FROM
    remove_dup_key_from_mapped_table AS mt
