DESCRIBE EXTENDED system.access.audit;
SELECT DISTINCT service_name
FROM system.access.audit;
SELECT *
FROM system.access.audit
WHERE
    service_name = 'clusterPolicies'
    AND action_name = 'create'
LIMIT 100;
