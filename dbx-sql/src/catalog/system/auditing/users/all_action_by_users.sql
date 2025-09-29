SELECT
    audit.event_time,
    audit.action_name,
    audit.request_params
FROM
    system.access.audit
WHERE
    user_identity.email = 'mohammadmahfooz.alam@gainwelltechnologies.com'
    AND request_params.catalog_name = 'system_views'
    AND audit.event_time >= now() - INTERVAL 20 DAYS
ORDER BY
    audit.event_time DESC;
