-- ============================================================
-- Topic: System catalog observability — audit, billing, compute, jobs, model
--   serving, lineage, query history
-- Dialect: Databricks Runtime (Unity Catalog) — NOT open-source Spark
-- Description: Read-only reference queries against the account-wide `system` catalog
--   and `information_schema` — the same tables backing the `dbx_mcp` server's
--   `query_system_table` tool. Schemas shown are Databricks' standard, publicly
--   documented system table layouts (no live workspace data is used here).
-- ============================================================

-- [Databricks] Requires Unity Catalog. The `system` catalog is Databricks-hosted,
-- populated automatically, and read-only — there is nothing to CREATE/DROP here,
-- only SELECT-only reference queries against tables that already exist in every
-- Unity Catalog-enabled workspace (subject to the caller having SELECT granted on
-- the relevant `system.*` schema).
--
-- PII note: several system tables carry a principal identity column (email or
-- username) — e.g. user_identity.email, executed_by, owned_by, creator_user_name,
-- run_as_user_name, requester. Every query below that needs to group or count by
-- "who" uses SHA2(<identity_column>, 256) to produce a stable, one-way pseudonym
-- instead of the raw email/username, so results stay joinable/comparable across
-- rows without ever exposing personal data. For a durable, query-independent
-- guarantee, apply a Unity Catalog column mask on the identity column itself
-- (see "Column-Level Security — Column Masks" in docs/data-sources/catalog/unity.md)
-- so even an ad hoc `SELECT *` cannot leak it.

---------------------------------------------------------------------------------------------------
-- 1. Catalog introspection — information_schema
--    information_schema exists in every catalog (including `system` itself) and
--    describes that catalog's own objects and privileges.
---------------------------------------------------------------------------------------------------

-- All tables in a catalog, with their type (managed Delta / view / external / streaming)
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_type,
    t.comment
FROM system.information_schema.tables AS t
ORDER BY t.table_schema, t.table_name;

-- Every column, with data type and nullability
SELECT
    c.table_name,
    c.column_name,
    c.ordinal_position,
    c.data_type,
    c.is_nullable
FROM system.information_schema.columns AS c
WHERE c.table_schema = 'access'
ORDER BY c.table_name, c.ordinal_position;

-- Who can query the audit table
SELECT
    p.grantee,
    p.privilege_type
FROM system.information_schema.table_privileges AS p
WHERE p.table_schema = 'access' AND p.table_name = 'audit';

---------------------------------------------------------------------------------------------------
-- 2. Cost monitoring — system.billing.usage
--    One row per usage record (DBUs, storage, network); usage_metadata is a struct
--    identifying which compute resource generated the usage.
---------------------------------------------------------------------------------------------------

-- DBUs consumed per product today
SELECT
    u.billing_origin_product,
    u.usage_date,
    u.usage_unit,
    SUM(u.usage_quantity) AS total_quantity
FROM system.billing.usage AS u
WHERE u.usage_date = CURRENT_DATE()
GROUP BY u.billing_origin_product, u.usage_date, u.usage_unit
ORDER BY total_quantity DESC;

-- Top jobs by DBU consumption over the last 7 days
SELECT
    u.usage_metadata.job_id, -- noqa: RF01
    SUM(u.usage_quantity) AS dbu_usage
FROM system.billing.usage AS u
WHERE
    u.usage_metadata.job_id IS NOT NULL -- noqa: RF01
    AND u.usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY u.usage_metadata.job_id -- noqa: RF01
ORDER BY dbu_usage DESC
LIMIT 10;

-- Attribute cost to a team via a custom cluster/job tag (custom_tags is MAP<STRING,STRING>)
SELECT
    u.sku_name,
    u.usage_unit,
    SUM(u.usage_quantity) AS total_quantity
FROM system.billing.usage AS u
WHERE u.custom_tags['team'] = 'data-eng'
GROUP BY u.sku_name, u.usage_unit
ORDER BY total_quantity DESC;

-- Expose every custom tag as its own row (map_entries + explode), for pivoting by tag key
SELECT
    u.usage_date,
    u.sku_name,
    u.usage_quantity,
    tag_entry.key AS tag_key, -- noqa: RF01
    tag_entry.value AS tag_value -- noqa: RF01
FROM system.billing.usage AS u
    LATERAL VIEW EXPLODE(MAP_ENTRIES(u.custom_tags)) as tag_entry
WHERE u.usage_date >= CURRENT_DATE() - INTERVAL 1 DAYS;

---------------------------------------------------------------------------------------------------
-- 3. Security & compliance auditing — system.access.audit
--    One row per auditable event; user_identity and response are structs.
---------------------------------------------------------------------------------------------------

-- Who dropped tables in a given catalog this week (actor pseudonymized, never raw email)
SELECT
    a.event_time,
    a.action_name,
    a.request_params,
    SHA2(a.user_identity.email, 256) AS actor_hash  -- noqa: RF01
FROM system.access.audit AS a
WHERE
    a.service_name = 'unityCatalog'
    AND a.action_name IN ('deleteTable', 'dropTable')
    AND a.event_time >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY a.event_time DESC;

-- Failed login attempts by source IP (potential brute-force indicator)
SELECT
    a.source_ip_address,
    SHA2(a.user_identity.email, 256) AS actor_hash,  -- noqa: RF01
    COUNT(*) AS attempts
FROM system.access.audit AS a
WHERE
    a.action_name = 'databricksAccountLogin'
    AND a.response.status_code != 200 -- noqa: RF01
GROUP BY a.source_ip_address, SHA2(a.user_identity.email, 256)  -- noqa: RF01
ORDER BY attempts DESC;

-- All grant/revoke activity on a catalog, most recent first
SELECT
    a.event_time,
    a.action_name,
    a.request_params,
    SHA2(a.user_identity.email, 256) AS actor_hash  -- noqa: RF01
FROM system.access.audit AS a
WHERE
    a.action_name IN ('updatePermissions', 'createGrant', 'deleteGrant')
    AND a.request_params['securable_type'] = 'CATALOG'
ORDER BY a.event_time DESC
LIMIT 50;

---------------------------------------------------------------------------------------------------
-- 4. Data lineage — system.access.table_lineage / column_lineage
--    Automatically captured for notebooks, jobs, and SQL warehouse queries — no
--    configuration required. Both tables list one row per source/target pair.
---------------------------------------------------------------------------------------------------

-- Everything downstream of a raw ingestion table (impact analysis before a schema change)
SELECT DISTINCT l.target_table_full_name
FROM system.access.table_lineage AS l
WHERE l.source_table_full_name = 'main.raw.events'
ORDER BY l.target_table_full_name;

-- Everything upstream of a reporting table (root-cause analysis for a bad value)
SELECT DISTINCT l.source_table_full_name
FROM system.access.table_lineage AS l
WHERE l.target_table_full_name = 'main.gold.daily_revenue'
ORDER BY l.source_table_full_name;

-- Column-level lineage: which source columns feed a specific target column
SELECT
    cl.source_table_full_name,
    cl.source_column_name
FROM system.access.column_lineage AS cl
WHERE
    cl.target_table_full_name = 'main.gold.daily_revenue'
    AND cl.target_column_name = 'revenue'
ORDER BY cl.source_table_full_name, cl.source_column_name;

---------------------------------------------------------------------------------------------------
-- 5. Query performance — system.query.history
--    One row per SQL statement executed on a SQL warehouse; compute is a struct
--    identifying the cluster or SQL warehouse that ran the statement.
---------------------------------------------------------------------------------------------------

-- Slowest queries on a given warehouse in the last 24 hours (submitter pseudonymized)
SELECT
    h.statement_id,
    h.total_duration_ms,
    h.statement_text,
    SHA2(h.executed_by, 256) AS executed_by_hash
FROM system.query.history AS h
WHERE
    h.compute.warehouse_id = '0123456789abcdef'  -- noqa: RF01
    AND h.start_time >= CURRENT_TIMESTAMP() - INTERVAL 1 DAYS
ORDER BY h.total_duration_ms DESC
LIMIT 20;

-- Queries that spilled to disk (memory pressure indicator)
SELECT
    h.statement_id,
    h.spilled_local_bytes,
    h.statement_text,
    SHA2(h.executed_by, 256) AS executed_by_hash
FROM system.query.history AS h
WHERE
    h.spilled_local_bytes > 0
    AND h.start_time >= CURRENT_TIMESTAMP() - INTERVAL 1 DAYS
ORDER BY h.spilled_local_bytes DESC
LIMIT 20;

-- Cache effectiveness: share of queries served from the result cache today
SELECT
    COUNT(*) AS total_queries,
    SUM(CAST(h.from_result_cache AS INT)) AS cache_hits,
    ROUND(SUM(CAST(h.from_result_cache AS INT)) / COUNT(*), 4) AS cache_hit_rate
FROM system.query.history AS h
WHERE h.start_time >= CURRENT_DATE();

---------------------------------------------------------------------------------------------------
-- 6. Compute inventory — system.compute.clusters
--    One row per compute definition change; delete_time IS NULL means still active.
---------------------------------------------------------------------------------------------------

-- Active cluster footprint by runtime, security mode, and creation source (no PII)
SELECT
    c.cluster_source,
    c.dbr_version,
    c.data_security_mode,
    COUNT(DISTINCT c.cluster_id) AS cluster_count
FROM system.compute.clusters AS c
WHERE c.delete_time IS NULL
GROUP BY c.cluster_source, c.dbr_version, c.data_security_mode
ORDER BY cluster_count DESC;

-- Cluster ownership by pseudonymized owner (owned_by is a username — never select raw)
SELECT
    SHA2(c.owned_by, 256) AS owner_hash,
    COUNT(DISTINCT c.cluster_id) AS cluster_count
FROM system.compute.clusters AS c
WHERE c.delete_time IS NULL
GROUP BY SHA2(c.owned_by, 256)
ORDER BY cluster_count DESC
LIMIT 10;

---------------------------------------------------------------------------------------------------
-- 7. SQL warehouse inventory — system.compute.warehouses
--    One row per warehouse definition change; delete_time IS NULL means still active.
---------------------------------------------------------------------------------------------------

-- Warehouse fleet composition by type and size (capacity-planning snapshot)
SELECT
    w.warehouse_type,
    w.warehouse_size,
    COUNT(DISTINCT w.warehouse_id) AS warehouse_count
FROM system.compute.warehouses AS w
WHERE w.delete_time IS NULL
GROUP BY w.warehouse_type, w.warehouse_size
ORDER BY warehouse_count DESC;

-- Warehouse creators, pseudonymized (created_by is a principal name — never select raw)
SELECT
    SHA2(w.created_by, 256) AS created_by_hash,
    COUNT(DISTINCT w.warehouse_id) AS warehouse_count
FROM system.compute.warehouses AS w
WHERE w.delete_time IS NULL
GROUP BY SHA2(w.created_by, 256)
ORDER BY warehouse_count DESC
LIMIT 10;

---------------------------------------------------------------------------------------------------
-- 8. Job inventory — system.lakeflow.jobs
--    One row per job definition change; delete_time IS NULL means still active.
---------------------------------------------------------------------------------------------------

-- Active jobs by trigger type and paused state (scheduling health snapshot)
SELECT
    j.paused,
    COALESCE(j.trigger_type, 'API_OR_LEGACY') AS trigger_type,
    COUNT(DISTINCT j.job_id) AS job_count
FROM system.lakeflow.jobs AS j
WHERE j.delete_time IS NULL
GROUP BY j.trigger_type, j.paused
ORDER BY job_count DESC;

-- Job authorship, pseudonymized (creator_user_name is an email/principal — never select raw)
SELECT
    SHA2(j.creator_user_name, 256) AS creator_hash,
    COUNT(DISTINCT j.job_id) AS job_count
FROM system.lakeflow.jobs AS j
WHERE j.delete_time IS NULL AND j.creator_user_name IS NOT NULL
GROUP BY SHA2(j.creator_user_name, 256)
ORDER BY job_count DESC
LIMIT 10;

---------------------------------------------------------------------------------------------------
-- 9. Model serving — system.serving.endpoint_usage / served_entities
--    One row per inference request; requester is a principal name/email.
---------------------------------------------------------------------------------------------------

-- Endpoint traffic health by HTTP status code, with average token volume
SELECT
    e.status_code,
    COUNT(*) AS request_count,
    ROUND(AVG(e.input_token_count), 0) AS avg_input_tokens,
    ROUND(AVG(e.output_token_count), 0) AS avg_output_tokens
FROM system.serving.endpoint_usage AS e
WHERE e.request_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY e.status_code
ORDER BY request_count DESC;

-- Top callers, pseudonymized (requester is a principal name/email — never select raw)
SELECT
    SHA2(e.requester, 256) AS requester_hash,
    COUNT(*) AS request_count
FROM system.serving.endpoint_usage AS e
WHERE
    e.request_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    AND e.requester IS NOT NULL
GROUP BY SHA2(e.requester, 256)
ORDER BY request_count DESC
LIMIT 10;
