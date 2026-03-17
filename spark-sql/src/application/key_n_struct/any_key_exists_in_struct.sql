SELECT
    *,
    map_entries(u.custom_tags) AS custom_tag
FROM system.billing.usage AS u
WHERE u.usage_date > current_date() - 1
