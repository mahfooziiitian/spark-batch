SELECT
    catalog_name,
    schema_name,
    map_from_entries(
        collect_list(named_struct('key', tag_name, 'value', tag_value))
    )
FROM system.information_schema.schema_tags
WHERE catalog_name = 'mgmt_stg'
GROUP BY catalog_name, schema_name;
