SELECT
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM
    system.information_schema.table_tags;
