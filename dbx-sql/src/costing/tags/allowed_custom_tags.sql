DESCRIBE EXTENDED system.billing.usage;
-- List all tags keys from last 30 days of usage data
SELECT DISTINCT tag_key
FROM
    system.billing.usage
        LATERAL VIEW explode(u.custom_tags) as tag_key,
        tag_value
WHERE usage_date >= date_sub(current_date, 30);
-- Exception to allowed custom tags keys
WITH usage_wrong_tag_key AS (
    SELECT DISTINCT
        usage.workspace_id,
        usage.tag_key,
        usage.sku_name,
        usage.billing_origin_product,
        coalesce(
            usage_metadata.job_id,
            usage_metadata.cluster_id,
            usage_metadata.warehouse_id
        ) AS compute_id
    FROM
        system.billing.usage
            LATERAL VIEW explode(u.custom_tags) as tag_key,
            tag_value
    WHERE
        usage.usage_date >= date_sub(current_date, 30)
        AND usage.tag_key NOT IN (
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
        )
)

SELECT
    w.workspace_name,
    u.workspace_id,
    u.tag_key,
    u.billing_origin_product,
    u.compute_id
FROM usage_wrong_tag_key AS u
LEFT JOIN
    mgmt_stg.metadata.workspace_id_crosswalk_view AS w
    ON u.workspace_id = w.workspace_id
ORDER BY u.workspace_id
