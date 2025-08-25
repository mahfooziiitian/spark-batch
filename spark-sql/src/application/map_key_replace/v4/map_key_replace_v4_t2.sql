-- Step-by-Step Strategy
-- Explode the map into key-value pairs
-- Apply the key replacement logic
-- Group by the new key and take the last value
-- Reconstruct the map

-- Case: 1 One key with one key in source table
WITH map_table AS (
    SELECT
        map(' Tenant', 'Tenant', 'Tent', 'Tenant', 'Environ', 'Env') AS key_map,
        map(' Tenant', '1', 'c', '3', 'Environ', 'prod') AS original_map
)

SELECT
    mt.key_map,
    mt.original_map,
    map_from_entries(
        aggregate(
            aggregate(
                transform(
                    map_entries(mt.original_map),
                    kv -> struct( --noqa
                        coalesce( mt.key_map[kv.key], kv.key ) as new_key,--noqa
                        kv.key as original_key,--noqa
                        kv.value as value--noqa
                    )
                ),
                cast(
                    array() AS ARRAY<STRUCT<new_key: string, value: string>>
                ),
                (acc, kv) -> if(--noqa
                    array_contains(map_keys(mt.original_map), kv.new_key)--noqa
                    AND kv.original_key != kv.new_key,
                    acc,--noqa
                    -- skip remapped key if it collides with original
                    acc || array(struct(kv.new_key, kv.value))--noqa
                )
            ),
            cast(
                array() AS ARRAY<STRUCT<new_key: string, value: string>>
            ),
            (acc, kv) -> filter(--noqa
                acc || array(kv),--noqa
                x -> x.new_key != kv.new_key OR x == kv --noqa
            )
        )
    ) AS final_map
FROM map_table AS mt;

-- =========Analysis================
WITH map_table AS (
    SELECT
        map(' Tenant', 'Tenant', 'Tent', 'Tenant', 'Environ', 'Env') AS key_map,
        map(' Tenant', '1', 'c', '3', 'Environ', 'prod') AS original_map
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
