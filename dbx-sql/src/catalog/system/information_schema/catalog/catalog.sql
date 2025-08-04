DESCRIBE TABLE system.information_schema.catalogs;
-- List All Catalogs with Their Owners
SELECT
    catalog_name,
    catalog_owner,
    created,
    last_altered
FROM system.information_schema.catalogs
ORDER BY created DESC;
--  Find Catalogs Created After a Specific Date
SELECT
    catalog_name,
    catalog_owner,
    created
FROM system.information_schema.catalogs
WHERE created > DATE('2025-01-01')
ORDER BY created DESC;
-- Catalogs with No Comments (Missing Documentation)
SELECT catalog_name
FROM system.information_schema.catalogs
WHERE
    comment IS NULL
    OR comment = '';
-- Join Catalogs with Schemas and Table Counts
WITH schema_counts AS (
    SELECT
        table_catalog,
        table_schema,
        COUNT(*) AS table_count
    FROM system.information_schema.tables
    WHERE table_type <> 'VIEW'
    GROUP BY
        table_catalog,
        table_schema
)

SELECT
    c.catalog_name,
    sc.table_schema,
    sc.table_count,
    c.catalog_owner
FROM system.information_schema.catalogs AS c
LEFT JOIN schema_counts AS sc ON c.catalog_name = sc.table_catalog
ORDER BY
    c.catalog_name,
    sc.table_schema;
-- Catalogs with Specific Owner
SELECT
    catalog_name,
    catalog_owner,
    created
FROM system.information_schema.catalogs
WHERE catalog_owner = 'admin'
ORDER BY created DESC;
-- Find Catalogs Accessible by a Specific User
SELECT
    catalog_name,
    catalog_owner
FROM system.information_schema.catalogs
WHERE catalog_name IN (
    SELECT catalog_name
    FROM system.information_schema.table_privileges
    WHERE grantee = 'mohammadmahfooz.alam@gainwelltechnologies.com'
);
