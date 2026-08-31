-- ============================================================
-- Topic: Application — map key replacement v4
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Applies a mapping table to usage custom tags and rebuilds the map.
-- ============================================================

WITH old_tag_mapping_key AS (
    SELECT
        map_from_entries(
            collect_list(struct(old_tag_key, new_tag_key))
        ) AS key_map
    FROM mgmt_stg.metadata.old_custom_tags_key_mapping
),

normalized_usage AS (
    SELECT
        u.*, --noqa: AM04
        map_from_entries(
            aggregate(
                aggregate(
                    transform(
                        map_entries(u.custom_tags),
                        kv -> struct(
                            coalesce(km.key_map[lower(trim(kv.key))], kv.key) as new_key, --noqa: RF01
                            kv.key as original_key, --noqa: RF01
                            kv.value as value --noqa: RF01
                        )
                    ),
                    cast(
                        array() AS ARRAY<STRUCT<new_key: string, original_key: string, value: string>>
                    ),
                    (acc, kv) -> if(
                        array_contains(map_keys(u.custom_tags), kv.new_key)
                            AND kv.original_key != kv.new_key, --noqa: RF01
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
        ) AS final_custom_tag
    FROM system.billing.usage AS u
    CROSS JOIN old_tag_mapping_key AS km
    WHERE u.usage_date > current_date() - 10
)

SELECT
    normalized_usage.* --noqa: AM04
FROM normalized_usage;
