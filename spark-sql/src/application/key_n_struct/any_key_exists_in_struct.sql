SELECT *,
  map_entries(custom_tags) AS custom_tag
FROM system.billing.usage u
WHERE u.usage_date > current_date() -1
