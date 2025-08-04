SELECT DISTINCT
    cp.grantee,
    cp.privilege_type,
    'catalog' AS level
FROM system.information_schema.catalog_privileges AS cp
WHERE cp.catalog_name = :catalog_name
UNION ALL
SELECT DISTINCT
    sp.grantee,
    sp.privilege_type,
    'schema' AS level
FROM system.information_schema.schema_privileges AS sp
WHERE sp.catalog_name = :catalog_name
    AND sp.schema_name = :schema_name
UNION ALL
SELECT DISTINCT
    tp.grantee AS `accessible by`,
    tp.privilege_type,
    'table' AS level
FROM system.information_schema.table_privileges AS tp
WHERE tp.table_catalog = :catalog_name
    AND tp.table_schema = :schema_name
    AND tp.table_name = :table_name
UNION ALL
SELECT
    t.table_owner,
    'ALL_PRIVILEGES' AS privilege_type,
    'owner' AS level
FROM system.information_schema.tables AS t
WHERE t.table_catalog = :catalog_name
    AND t.table_schema = :schema_name
    AND t.table_name = :table_name;

--  Find All Tables a User Has Access To
SELECT p.*
FROM system.information_schema.table_privileges AS p
WHERE p.grantee = 'mohammadmahfooz.alam@gainwelltechnologies.com';

-- Find All Nullable Columns
SELECT
    c.table_schema,
    c.table_name,
    c.column_name
FROM system.information_schema.columns AS c
WHERE
    c.is_nullable = 'YES';
