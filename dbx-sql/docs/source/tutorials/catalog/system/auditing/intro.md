# Introduction

Includes records for all audit events from workspaces in your region.

audit log table schema and has sample queries you can use with the audit log system table to answer common account activity questions.

`Table path`: system.access.audit

## Audit log considerations

1. Most audit logs are only available in the region of the workspace.
2. Account-level audit logs record workspace_id as 0.

## Who can access this table?

```sql
SELECT DISTINCT(grantee), privilege_type, 'catalog' AS level
FROM system.information_schema.catalog_privileges
WHERE
  catalog_name = :catalog_name
UNION
SELECT DISTINCT(grantee), privilege_type, 'schema' AS level
FROM system.information_schema.schema_privileges
WHERE
  catalog_name = :catalog_name AND schema_name = :schema_name
UNION
SELECT DISTINCT(grantee) AS `accessible by`, privilege_type, 'table' AS level
FROM
  system.information_schema.table_privileges
WHERE
  table_catalog = :catalog_name AND table_schema = :schema_name AND table_name = :table_name
UNION
SELECT table_owner, 'ALL_PRIVILEGES' AS privilege_type, 'owner' AS level
FROM system.information_schema.tables
WHERE
  table_catalog = :catalog_name AND table_schema = :schema_name AND table_name = :table_name
```

## Which users accessed a table within the last seven day?

```sql
SELECT
  user_identity.email as `User`,
  IFNULL(
    request_params.full_name_arg,
    request_params.name
  ) AS `Table`,
  action_name AS `Type of Access`,
  event_time AS `Time of Access`
FROM
  system.access.audit
WHERE
  (
    request_params.full_name_arg = :table_full_name
    OR (
      request_params.name = :table_name
      AND request_params.schema_name = :schema_name
    )
  )
  AND action_name IN ('createTable', 'getTable', 'deleteTable')
  AND event_date > now() - interval 7 day
ORDER BY
  event_date DESC
```

## Which tables did a user access recently?

```sql
SELECT
  action_name as `EVENT`,
  event_time as `WHEN`,
  IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
  IFNULL(request_params.commandText, 'GET table') AS `QUERY TEXT`
FROM
  system.access.audit
WHERE
  user_identity.email = :User
  AND action_name IN (
    'createTable',
    'commandSubmit',
    'getTable',
    'deleteTable'
  )
  AND datediff(now(), event_date) < :days_ago
ORDER BY
  event_date DESC
```

## View permissions changes for all securable objects

```sql
SELECT 
    event_time,
    user_identity.email,
    request_params.securable_type,
    request_params.securable_full_name,
    request_params.changes
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
ORDER BY 1 DESC
```

## View the most recently run notebook commands

```sql
SELECT event_time, user_identity.email, request_params.commandText
FROM system.access.audit
WHERE action_name = `runCommand`
ORDER BY event_time DESC
LIMIT 100
```

## Which users have logged into a Databricks app?

```sql
SELECT
  event_date,
  workspace_id,
  user_identity.email as user_email,
  user_identity.subject_name as username
FROM
  system.access.audit
WHERE
  action_name IN ("workspaceInHouseOAuthClientAuthentication", "mintOAuthToken", "mintOAuthAuthorizationCode")
AND
  request_params["client_id"] = "<oauth2-app-client-id>"
GROUP BY
  event_date,
  workspace_id,
  user_email,
  username
```