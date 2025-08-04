# Privelege

```sql
SELECT DISTINCT(grantee), privilege_type, 'catalog' AS level
FROM system.information_schema.catalog_privileges
WHERE
  catalog_name = :catalog_name
UNION
SELECT DISTINCT(grantee), privilege_type, 'schema' AS level
FROM system.information_schema.schema_privileges
WHERE
  catalog_name = :catalog_name AND schema_name = :schema_name
UNION
SELECT DISTINCT(grantee) AS `accessible by`, privilege_type, 'table' AS level
FROM
  system.information_schema.table_privileges
WHERE
  table_catalog = :catalog_name AND table_schema = :schema_name AND table_name = :table_name
UNION
SELECT table_owner, 'ALL_PRIVILEGES' AS privilege_type, 'owner' AS level
FROM system.information_schema.tables
WHERE
  table_catalog = :catalog_name AND table_schema = :schema_name AND table_name = :table_name
```

