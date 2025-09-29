-- Audit who has accessed a specific table

SELECT
    user_identity.email,
    audit.event_time,
    request_params.table_full_name
FROM
    system.access.audit
WHERE
    request_params.table_full_name
    = 'dynamic_views.billing.usage'
    AND audit.action_name = 'generateTemporaryTableCredential'
    AND audit.event_time > NOW() - INTERVAL 30 DAYS
ORDER BY
    audit.event_time DESC;


-- Audit tables dropped within the last 7 days from any schema
SELECT
    audit.event_date,
    audit.event_time,
    audit.action_name,
    user_identity.email,
    identity_metadata.run_by,
    identity_metadata.run_as,
    request_params.full_name_arg AS table_full_name
FROM
    system.access.audit
WHERE
    audit.action_name = 'deleteTable'
    AND audit.event_date > NOW() - INTERVAL 7 DAYS
    AND request_params.full_name_arg LIKE 'oz_dev.bronze_dss_nj%fact%'
    ---and request_params.catalog_name ='bronze_dss_nj'
ORDER BY
    audit.event_time DESC;
