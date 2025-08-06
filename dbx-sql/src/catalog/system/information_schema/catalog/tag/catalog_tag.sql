select 
    catalog_name,
    map(tag_name, tag_value)
FROM system.information_schema.catalog_tags
where catalog_name = 'mgmt_stg' and tag_name is not null;

select 
    catalog_name,
   named_struct('key', tag_name, 'value', tag_value)  
FROM system.information_schema.catalog_tags
where catalog_name = 'mgmt_stg'
and tag_name is not null;

select 
    catalog_name,
    map_from_entries(collect_list(named_struct('key', tag_name, 'value', tag_value)))  
FROM system.information_schema.catalog_tags
where catalog_name = 'mgmt_stg'
GROUP BY catalog_name;

