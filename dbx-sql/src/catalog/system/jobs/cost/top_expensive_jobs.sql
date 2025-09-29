SELECT
    usage_metadata.job_id,
    SUM(usage.usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE
    usage.usage_start_time >= NOW() - INTERVAL 7 DAYS
    AND usage_metadata.job_id IS NOT NULL
GROUP BY 1
ORDER BY total_dbus DESC
LIMIT 10;
