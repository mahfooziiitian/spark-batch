# Table Tag

`INFORMATION_SCHEMA.TABLE_TAGS` contains the table tagging metadata within the table, or all tables if owned by the `SYSTEM` catalog. Also contains tagging metadata for views and materialized views.

Information is displayed only for catalogs the user has permission to interact with.

## Queries

```sql
SELECT
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM 
    information_schema.table_tags;
```
