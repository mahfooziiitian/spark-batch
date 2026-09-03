-- ============================================================
-- Topic: Application — map key replacement v2
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Renames map keys and keeps the last value for each resolved key.
-- ============================================================

-- Step-by-step strategy
-- Explode the map into key-value pairs
-- Apply the key replacement logic
-- Group by the new key and take the last value
-- Reconstruct the map
WITH map_table AS (
    SELECT
        map('a', 'x', 'b', 'x', 'd', 'y') AS key_map,
        map('a', '1', 'b', '2', 'c', '3', 'd', '4', 'y', '5') AS original_map
)

SELECT
    map_from_entries(
        aggregate(
            transform(
                map_entries(original_map),
                kv -> STRUCT(
                    coalesce(key_map[kv.key], kv.key) AS new_key, --noqa: RF01
                    kv.value AS value --noqa: RF01
                )
            ),
            cast(array() AS ARRAY<STRUCT<new_key: string, value: string>>),
            (acc, kv) -> filter(
                acc || array(kv),
                x -> x.new_key != kv.new_key OR x = kv --noqa: RF01
            )
        )
    ) AS final_map
FROM map_table;
