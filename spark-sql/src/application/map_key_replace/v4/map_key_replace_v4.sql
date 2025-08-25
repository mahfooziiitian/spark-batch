WITH old_tag_mapping_key AS (
    SELECT
        map_from_entries(
            collect_list(struct(old_tag_key, new_tag_key))
        ) AS key_map
    FROM
        mgmt_stg.metadata.old_custom_tags_key_mapping
)

SELECT
    u.*,--noqa
    map_from_entries(
        aggregate(--noqa
            aggregate(--noqa
                transform(--noqa
                    map_entries(u.custom_tags),--noqa
                    kv -> struct(--noqa
                        coalesce(km.key_map[lower(trim(kv.key))], kv.key) as new_key,  --noqa
                        kv.key as original_key,--noqa
                        kv.value as value--noqa
                    )
                ),
                cast(array() AS ARRAY<STRUCT<new_key: string, value: string>>),--noqa
                (acc, kv) -> if(--noqa
                    array_contains(map_keys(u.custom_tags), kv.new_key) AND kv.original_key != kv.new_key,--noqa
                    acc, --noqa
                    acc || array(struct(kv.new_key, kv.value))--noqa
                )
            ),
            cast(array() AS ARRAY<STRUCT<new_key: string, value: string>>),--noqa
            (acc, kv) -> filter(--noqa
                acc || array(kv),--noqa
                x -> x.new_key != kv.new_key OR x == kv  --noqa
            )
        )
    ) AS final_custom_tag
FROM system.billing.usage AS u CROSS JOIN old_tag_mapping_key AS km
WHERE u.usage_date > current_date() - 10;
