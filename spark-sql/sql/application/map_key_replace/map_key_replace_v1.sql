-- Step-by-Step Strategy
-- Explode the map into key-value pairs
-- Apply the key replacement logic
-- Group by the new key and take the last value
-- Reconstruct the map
WITH exploded AS (
    SELECT
        key_map,
        my_map,
        explode(my_map) AS (orig_key, value)
    FROM your_table
),

replaced AS (
    SELECT
        value,
        CASE
            WHEN key_map[orig_key] IS NOT NULL THEN key_map[orig_key]
            ELSE orig_key
        END AS new_key
    FROM exploded
),

deduplicated AS (
    SELECT
        new_key,
        last(value) OVER (
            PARTITION BY new_key
            ORDER BY some_ordering_column
        ) AS final_value
    FROM replaced
)

SELECT map_from_entries(collect_list(struct(new_key, final_value))) AS final_map
FROM deduplicated;
