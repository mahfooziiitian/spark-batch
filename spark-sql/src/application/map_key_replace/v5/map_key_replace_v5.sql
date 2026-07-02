-- ============================================================
-- Topic: Application — map key replacement v5
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Renames keys, skips conflicting originals, and keeps final distinct entries.
-- ============================================================

-- Step-by-step strategy
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
            aggregate(
                transform(
                    map_entries(original_map),
                    kv -> struct(
                        coalesce(key_map[kv.key], kv.key) as new_key, --noqa: RF01
                        kv.key as original_key, --noqa: RF01
                        kv.value as value --noqa: RF01
                    )
                ),
                cast(
                    array() AS ARRAY<STRUCT<new_key: string, original_key: string, value: string>>
                ),
                (acc, kv) -> if(
                    exists(
                        filter(
                            acc,
                            x -> x.new_key = kv.new_key AND x.original_key != kv.original_key --noqa: RF01
                        )
                    ) AND kv.original_key != kv.new_key, --noqa: RF01
                    acc,
                    acc || array(struct(kv.new_key, kv.original_key, kv.value)) --noqa: RF01
                )
            ),
            cast(
                array() AS ARRAY<STRUCT<new_key: string, original_key: string, value: string>>
            ),
            (acc, kv) -> filter(
                acc || array(kv),
                x -> x.new_key != kv.new_key OR x = kv --noqa: RF01
            )
        )
    ) AS final_map
FROM map_table;
