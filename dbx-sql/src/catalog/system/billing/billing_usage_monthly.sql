WITH rest_price_usage AS (
    SELECT
        u.workspace_id,
        u.usage_date AS ds,
        CAST(u.usage_quantity AS DOUBLE) AS dbus,
        CAST(lp.pricing.default * u.usage_quantity AS DOUBLE)
            AS cost_at_list_price,
        COALESCE(
            CASE
                WHEN u.custom_tags.tenant IN ('oh', 'haven') THEN 'oh_haven'
                WHEN u.custom_tags.tenant = 'oh-epa' THEN 'oh_epa'
                WHEN u.custom_tags.tenant = 'Texas' THEN 'tx'
                WHEN u.custom_tags.tenant = 'Insight' THEN 'insight'
                WHEN
                    u.custom_tags.tenant = 'insight_base_tables'
                    THEN 'insight_base'
                WHEN u.custom_tags.tenant = 'ca-uw2' THEN 'ca'
                WHEN u.custom_tags.tenant = 'de_edw' THEN 'de'
                WHEN u.custom_tags.tenant = 'hedis_ga_dev' THEN 'hedis_ga'
                WHEN u.custom_tags.tenant = 'hedis_ga_stg' THEN 'hedis_ga'
                WHEN u.custom_tags.tenant = 'hedis_oz_dev' THEN 'hedis_oz'
                WHEN u.custom_tags.tenant = 'hedis_nm_dev' THEN 'hedis_nm'
                WHEN u.custom_tags.tenant = 'MS' THEN 'hedis_ms'
                WHEN
                    u.workspace_id = '67969358316836'
                    AND u.custom_tags.tenant = 'tx' THEN 'insight_tx'
                WHEN
                    u.workspace_id = '5048061916187028'
                    AND u.custom_tags.tenant = 'oz' THEN 'insight_ky'
                WHEN
                    u.workspace_id = '7592211574530171'
                    AND u.custom_tags.tenant = 'tx' THEN 'insight_tx'
                WHEN
                    u.workspace_id = '2762755653818796'
                    AND u.custom_tags.tenant = 'tenant' THEN 'ks'
                WHEN
                    u.workspace_id = '487358706119890'
                    AND u.custom_tags.tenant = 'tenant' THEN 'ks'
                WHEN
                    u.workspace_id = '3114357280267733'
                    AND u.custom_tags.tenant = 'oz' THEN 'tx'
                ELSE LOWER(REPLACE(u.custom_tags.tenant, '-', '_'))
            END,
            'NULL'
        ) AS tenant,
        CASE
            WHEN CONTAINS(u.sku_name, 'ALL_PURPOSE') THEN 'All Purpose'
            WHEN CONTAINS(u.sku_name, 'JOBS') THEN 'Jobs'
            WHEN
                CONTAINS(u.sku_name, 'SQL')
                AND NOT CONTAINS(u.sku_name, 'SERVERLESS') THEN 'SQL Compute'
            WHEN
                CONTAINS(u.sku_name, 'SQL')
                AND CONTAINS(u.sku_name, 'SERVERLESS')
                THEN 'Serverless SQL Compute'
            WHEN CONTAINS(u.sku_name, 'INFERENCE') THEN 'Model Inference'
            WHEN CONTAINS(u.sku_name, 'DLT') THEN 'Delta Live tables'
            ELSE 'Other'
        END AS sku
    FROM system.billing.usage AS u
    INNER JOIN system.billing.list_prices AS lp
        ON
            u.cloud = lp.cloud
            AND u.sku_name = lp.sku_name
            AND u.usage_start_time >= lp.price_start_time
            AND (
                u.usage_end_time <= lp.price_end_time
                OR lp.price_end_time IS NULL
            )
    WHERE
        u.usage_unit = 'DBU'
        AND u.workspace_id IN (
            '4452300913827223',
            '7733707694417143',
            '1622554106049495',
            '7851233107767998',
            '222635236754985',
            '2233432169073953'
        )
        AND u.usage_date
        >= LAST_DAY(ADD_MONTHS(CURRENT_DATE, -1500)) + INTERVAL 1 DAY
        AND u.usage_date < DATE_TRUNC('month', CURRENT_DATE)
),

rest_price_usage_catalog_workspace (
    SELECT
        c.*,
        b.workspace_name,
        b.env
    FROM rest_price_usage AS c
    LEFT JOIN
        mgmt_stg.metadata.workspace_id_crosswalk_view AS b
        ON c.workspace_id = b.workspace_id
),

price_usage_tenant (
    SELECT
        c.workspace_id,
        c.workspace_name,
        c.ds,
        c.sku,
        c.cost_at_list_price,
        b.tenant,
        c.env,
        'REST' AS usage_type
    FROM rest_price_usage_catalog_workspace AS c
    LEFT JOIN mgmt_stg.metadata.workspace_id_crosswalk_tenant_view
        AS b ON c.tenant = b.tenant
    AND c.workspace_id = b.workspace_id
),

usage_pivot AS (
    SELECT
        tenant,
        ds AS usage_date,
        workspace_id,
        workspace_name,
        env,
        sku,
        usage_type,
        DATE_FORMAT(ds, 'MMM') AS usage_month,
        SUM(cost_at_list_price) AS cost
    FROM price_usage_tenant
    GROUP BY
        tenant,
        ds,
        DATE_FORMAT(ds, 'MMM'),
        workspace_id,
        workspace_name,
        env,
        sku,
        usage_type
    ORDER BY
        workspace_id,
        ds
),

usage_per_workspace_per_sku AS (
    SELECT *
    FROM usage_pivot PIVOT (
        SUM(cost) FOR (sku) IN (
            'All Purpose',
            'Jobs',
            'SQL Compute',
            'Serverless SQL Compute',
            'Model Inference',
            'Delta Live tables',
            'Other'
        )
    )
)

SELECT
    u.workspace_name,
    u.workspace_id,
    u.env,
    --u.usage_type,
    u.usage_month,
    YEAR(u.usage_date) AS usage_year,
    CAST(
        SUM(
            COALESCE(u.`All Purpose`, 0)
            + COALESCE(u.`Jobs`, 0)
            + COALESCE(u.`SQL Compute`, 0)
            + COALESCE(u.`Serverless SQL Compute`, 0)
            + COALESCE(u.`Model Inference`, 0)
            + COALESCE(u.`Delta Live tables`, 0)
            + COALESCE(u.`Other`, 0)
        ) AS DECIMAL(10, 4)
    ) AS `Total Dollar Spent`,
    CAST(
        COALESCE(SUM(u.`All Purpose`), 0) AS DECIMAL(10, 4)
    ) AS `All Purpose`,
    CAST(COALESCE(SUM(u.`Jobs`), 0) AS DECIMAL(10, 4)) AS `Jobs`,
    CAST(
        COALESCE(SUM(u.`SQL Compute`), 0) AS DECIMAL(10, 4)
    ) AS `SQL Compute`,
    CAST(
        COALESCE(SUM(u.`Serverless SQL Compute`), 0) AS DECIMAL(10, 4)
    ) AS `Serverless SQL Compute`,
    CAST(
        COALESCE(SUM(u.`Model Inference`), 0) AS DECIMAL(10, 4)
    ) AS `Model Inference`,
    CAST(
        COALESCE(SUM(u.`Delta Live tables`), 0) AS DECIMAL(10, 4)
    ) AS `Delta Live tables`,
    CAST(COALESCE(SUM(u.`Other`), 0) AS DECIMAL(10, 4)) AS `Other`
FROM usage_per_workspace_per_sku AS u
GROUP BY
    u.workspace_name,
    u.workspace_id,
    u.env,
    --u.usage_type,
    YEAR(u.usage_date),
    u.usage_month
ORDER BY u.workspace_name
