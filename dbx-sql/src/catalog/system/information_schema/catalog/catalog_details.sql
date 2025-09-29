SELECT * FROM system.information_schema.catalogs;

DESCRIBE EXTENDED CATALOG travel_delta_share;

-- SELECT * 
-- FROM system.information_schema.catalogs
-- WHERE catalog_name = 'travel_delta_share';

-- SHOW CATALOGS;

SELECT concat('DESCRIBE CATALOG EXTENDED ', catalog_name, ';') AS describe_stmt
FROM system.information_schema.catalogs;

-- SELECT
--   catalog_name,
--   catalog_owner,
--   properties['share_name'] AS share_name,
--   properties['provider_name'] AS provider_name
-- FROM system.information_schema.catalogs;

SELECT *
FROM system.information_schema.catalog_provider_share_usage;

SELECT *
FROM system.information_schema.shares;
