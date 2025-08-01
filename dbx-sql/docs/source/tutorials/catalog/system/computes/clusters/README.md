# Clusters

## Join cluster records with the most recent billing records

```sql
SELECT
  u.record_id,
  c.cluster_id,
  c.owned_by,
  c.change_time,
  u.usage_start_time,
  u.usage_quantity
FROM
  system.billing.usage u
  JOIN system.compute.clusters c
  JOIN (SELECT u.record_id, c.cluster_id, max(c.change_time) change_time
    FROM system.billing.usage u
    JOIN system.compute.clusters c
    WHERE
      u.usage_metadata.cluster_id is not null
      and u.usage_start_time >= '2023-01-01'
      and u.usage_metadata.cluster_id = c.cluster_id
      and date_trunc('HOUR', c.change_time) <= date_trunc('HOUR', u.usage_start_time)
    GROUP BY all) config
WHERE
  u.usage_metadata.cluster_id is not null
  and u.usage_start_time >= '2023-01-01'
  and u.usage_metadata.cluster_id = c.cluster_id
  and u.record_id = config.record_id
  and c.cluster_id = config.cluster_id
  and c.change_time = config.change_time
ORDER BY cluster_id, usage_start_time desc;
```

## Identify the compute resources with the highest average utilization and peak utilization

```sql
SELECT
        distinct cluster_id,
driver,
avg(cpu_user_percent + cpu_system_percent) as `Avg CPU Utilization`,
max(cpu_user_percent + cpu_system_percent) as `Peak CPU Utilization`,
        avg(cpu_wait_percent) as `Avg CPU Wait`,
        max(cpu_wait_percent) as `Max CPU Wait`,
        avg(mem_used_percent) as `Avg Memory Utilization`,
        max(mem_used_percent) as `Max Memory Utilization`,
avg(network_received_bytes)/(1024^2) as `Avg Network MB Received per Minute`,
avg(network_sent_bytes)/(1024^2) as `Avg Network MB Sent per Minute`
FROM
        node_timeline
WHERE
        start_time >= date_add(now(), -1)
GROUP BY
        cluster_id,
        driver
ORDER BY
        3 desc;
```