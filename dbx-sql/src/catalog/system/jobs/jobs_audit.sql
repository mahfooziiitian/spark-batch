SELECT
    a.*,
    a.event_time
FROM system.access.audit AS a
WHERE
    a.service_name = 'jobs'
    AND a.event_date > CURRENT_DATE - INTERVAL '3' DAY
ORDER BY a.event_time DESC;
