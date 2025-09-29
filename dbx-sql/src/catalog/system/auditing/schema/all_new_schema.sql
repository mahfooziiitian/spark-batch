-- desc system.access.audit; 

SELECT
    audit.event_time,
    user_identity.email AS created_by,
    identity_metadata.run_by,
    identity_metadata.run_as,
    request_params.name AS schema_nm,
    request_params.catalog_name AS catalog_nm,
    request_params.workspace_id,
    request_params.metastore_id,
    request_params.storage_root
FROM
    system.access.audit
WHERE
    audit.action_name = 'createSchema'
    AND audit.event_time >= now() - INTERVAL 2 DAYS
ORDER BY
    audit.event_time DESC;
