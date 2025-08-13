SELECT
    catalog_name,
    map(tag_name, tag_value)
FROM system.information_schema.catalog_tags
WHERE catalog_name = 'mgmt_stg' AND tag_name IS NOT null;

SELECT
    catalog_name,
    named_struct('key', tag_name, 'value', tag_value)
FROM system.information_schema.catalog_tags
WHERE
    catalog_name = 'mgmt_stg'
    AND tag_name IS NOT null;

SELECT
    catalog_name,
    map_from_entries(
        collect_list(named_struct('key', tag_name, 'value', tag_value))
    )
FROM system.information_schema.catalog_tags
WHERE catalog_name = 'mgmt_stg'
GROUP BY catalog_name;
