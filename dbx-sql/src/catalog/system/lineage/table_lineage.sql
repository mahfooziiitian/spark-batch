SHOW TABLES IN system.access;
DESCRIBE EXTENDED system.access.table_lineage;

-- Job run id
SELECT * --noqa
FROM system.access.table_lineage
WHERE entity_run_id = '602427424354474';


SELECT * --noqa
FROM system.access.table_lineage
WHERE entity_run_id = '602427424354474';


-- task run id

SELECT * --noqa
FROM system.access.table_lineage
WHERE
    entity_run_id = '1009457608740283'
    AND event_date > current_date() - 30;

SELECT * --noqa
FROM system.access.table_lineage
WHERE entity_id = '225924957117969' AND event_date > current_date() - 30;
