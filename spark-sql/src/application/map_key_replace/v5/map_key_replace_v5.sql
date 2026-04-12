-- Step-by-Step Strategy
-- Explode the map into key-value pairs
-- Apply the key replacement logic
-- Group by the new key and take the last value
-- Reconstruct the map

WITH map_table AS (
    SELECT
        map('a', 'x', 'b', 'x', 'd', 'y') AS key_map,
        map('a', '1', 'b', '2', 'c', '3', 'y', '5', 'd', '4') AS original_map
)

SELECT
    map_from_entries(
        aggregate(
            transform(
                map_entries(map_table.original_map),
                map_table.kv -> struct(
                    coalesce(
                        map_table.key_map[kv.key], kv.key
                    ) as map_table.new_key,
                    kv.key as map_table.original_key,
                    kv.value as map_table.value
                )
            ),
            cast(
                array() AS ARRAY<STRUCT<new_key: string, value: string>>
            ),
            (map_table.acc, map_table.kv)
            -- Skip if new_key already exists in accumulator from a different original key
            -> if(
                exists(
                    filter(
                        map_table.acc,
                        map_table.x -> x.new_key = kv.new_key
                        AND x.original_key != kv.original_key
                    )
                ) AND kv.original_key != kv.new_key,
                map_table.acc,
                map_table.acc
                || array(struct(kv.new_key, kv.original_key, kv.value))
            )
        )
    ) AS final_map
FROM map_table;
