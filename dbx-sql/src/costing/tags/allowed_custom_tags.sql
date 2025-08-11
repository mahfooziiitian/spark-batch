DESCRIBE EXTENDED system.billing.usage;
-- List all tags keys from last 30 days of usage data
SELECT DISTINCT tag_key
FROM system.billing.usage u LATERAL VIEW explode(u.custom_tags) AS tag_key,
    tag_value
WHERE usage_date >= date_sub(current_date, 30);
-- Exception to allowed custom tags keys
with usage_wrong_tag_key AS (SELECT DISTINCT workspace_id,
    tag_key,
    sku_name,
    billing_origin_product,
    coalesce(
        usage_metadata.job_id,
        usage_metadata.cluster_id,
        usage_metadata.warehouse_id
    ) AS compute_id
FROM system.billing.usage u LATERAL VIEW explode(u.custom_tags) AS tag_key,
    tag_value
WHERE usage_date >= date_sub(current_date, 30)
    AND tag_key NOT IN (
        'Aws_Account_Number',
        'EndpointId',
        'Project',
        'LakehouseMonitoringWorkspaceId',
        'Predictive Optimization',
        'Infra_Service',
        'ServingType',
        'Tenant',
        'CreatedBy',
        'Email',
        'App_Service',
        'Aws_Region',
        'Product',
        'Company',
        'LakehouseMonitoringTableId',
        'LakehouseMonitoringMetastoreId',
        'Env',
        'business_unit',
        'LakehouseMonitoring',
        'owner',
        'Team',
        'BudgetPolicyName',
        'BudgetPolicyId',
        'DATABRICKS_CATALOG'
))
SELECT 
    w.workspace_name,
    u.workspace_id,
    u.tag_key,
    u.billing_origin_product,
    u.compute_id
FROM usage_wrong_tag_key u left join mgmt_stg.metadata.workspace_id_crosswalk_view w
using (workspace_id)
order by u.workspace_id