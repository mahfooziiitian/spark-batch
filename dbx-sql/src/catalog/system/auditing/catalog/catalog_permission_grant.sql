-- Check who is granting permissions within a catalog
-- DESCRIBE system.access.audit;

SELECT DISTINCT action_name
FROM
    system.access.audit
WHERE
    service_name = 'unityCatalog' AND (event_time > now() - INTERVAL '10' DAY)
ORDER BY action_name DESC;



SELECT
    event_time,
    request_params.name_arg,
    request_params.isolation_mode,
    request_params.workspace_id,
    request_params.metastore_id
FROM
    system.access.audit
WHERE
    audit.action_name = 'updateCatalog'
    AND
    audit.service_name = 'unityCatalog'
    AND (audit.event_time > now() - INTERVAL '10' DAY)
ORDER BY audit.event_time DESC;
