-- ============================================================
-- Topic: Application — map keys inside structs
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Shows how to expose custom tag entries from usage records.
-- ============================================================

SELECT * --noqa: AM04
FROM (
    SELECT
        *, --noqa: AM04
        map_entries(custom_tags) AS custom_tag
    FROM system.billing.usage
    WHERE usage_date > current_date() - 1
) AS usage_tags;
