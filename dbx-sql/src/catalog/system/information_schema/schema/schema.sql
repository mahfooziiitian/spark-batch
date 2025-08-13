DESCRIBE TABLE system.information_schema.schemata;
SELECT schema_owner
FROM information_schema.schemata
WHERE
    schema_name = 'information_schema'
    AND catalog_name = 'main'
