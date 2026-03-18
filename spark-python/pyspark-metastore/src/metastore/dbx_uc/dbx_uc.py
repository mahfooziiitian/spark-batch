import os

from pyspark.sql import SparkSession

DBX_WORKSPACE_URL = os.environ.get(
    "DBX_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com"
)
UC_CATALOG = os.environ.get("UC_CATALOG", "main")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "analytics")


def create_spark_session():
    return (
        SparkSession.builder.appName("DatabricksUnityCatalog")
        .config("spark.databricks.unityCatalog.enabled", "true")
        .config("spark.databricks.unityCatalog.workspaceUrl", DBX_WORKSPACE_URL)
        .getOrCreate()
    )


def enable_unity_catalog(spark, workspace_url):
    spark.conf.set("spark.databricks.unityCatalog.enabled", "true")
    spark.conf.set("spark.databricks.unityCatalog.workspaceUrl", workspace_url)


def set_catalog_and_schema(spark, catalog_name, schema_name):
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")


def query_table(spark, table_name):
    df = spark.sql(f"SELECT * FROM {table_name}")
    df.show()
    df.printSchema()
    return df


def demonstrate_uc_governance(spark):
    print("=== Unity Catalog Governance ===")

    table_fqn = f"{UC_CATALOG}.{UC_SCHEMA}.events"

    print(f"\n-- Show grants on {table_fqn} --")
    spark.sql(f"SHOW GRANTS ON TABLE {table_fqn}").show(truncate=False)

    print("\n-- Grant/Revoke examples (requires appropriate privileges) --")
    grant_examples = [
        f"GRANT SELECT ON TABLE {table_fqn} TO `data-readers@example.com`",
        f"GRANT MODIFY ON TABLE {table_fqn} TO `data-writers@example.com`",
        f"REVOKE SELECT ON TABLE {table_fqn} FROM `data-readers@example.com`",
        f"GRANT USAGE ON SCHEMA {UC_CATALOG}.{UC_SCHEMA} TO `analysts@example.com`",
        f"GRANT USAGE ON CATALOG {UC_CATALOG} TO `analysts@example.com`",
    ]
    for stmt in grant_examples:
        print(f"  {stmt}")


def demonstrate_uc_three_level_namespace(spark):
    print("=== Unity Catalog Three-Level Namespace ===")

    print("\n-- Pattern: <catalog>.<schema>.<table> --")
    print(f"  Example: {UC_CATALOG}.{UC_SCHEMA}.events")

    print("\n-- Create catalog (requires UC metastore admin) --")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")

    print("\n-- Create schema --")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

    print("\n-- Create managed table --")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.events (
            event_id    BIGINT,
            event_type  STRING,
            user_id     BIGINT,
            payload     STRING,
            event_ts    TIMESTAMP
        )
    """)

    print("\n-- Insert sample data --")
    spark.sql(f"""
        INSERT INTO {UC_CATALOG}.{UC_SCHEMA}.events VALUES
            (1, 'click',    1001, '{{"page": "home"}}',    TIMESTAMP '2024-03-01 10:00:00'),
            (2, 'purchase', 1002, '{{"item": "widget"}}',  TIMESTAMP '2024-03-01 10:05:00'),
            (3, 'click',    1001, '{{"page": "product"}}', TIMESTAMP '2024-03-01 10:10:00'),
            (4, 'signup',   1003, '{{"source": "ad"}}',    TIMESTAMP '2024-03-01 10:15:00')
    """)

    print("\n-- Query with fully qualified name --")
    spark.sql(f"""
        SELECT event_type, COUNT(*) AS cnt
        FROM {UC_CATALOG}.{UC_SCHEMA}.events
        GROUP BY event_type
        ORDER BY cnt DESC
    """).show()

    print("\n-- List objects in the namespace --")
    spark.sql(f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}").show(truncate=False)


def demonstrate_uc_information_schema(spark):
    print("=== Unity Catalog Information Schema ===")

    print(f"\n-- Tables in {UC_CATALOG} via information_schema --")
    spark.sql(f"""
        SELECT table_catalog, table_schema, table_name, table_type
        FROM system.information_schema.tables
        WHERE table_catalog = '{UC_CATALOG}'
        ORDER BY table_schema, table_name
    """).show(truncate=False)

    print(f"\n-- Columns for {UC_CATALOG}.{UC_SCHEMA}.events --")
    spark.sql(f"""
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM system.information_schema.columns
        WHERE table_catalog = '{UC_CATALOG}'
          AND table_schema  = '{UC_SCHEMA}'
          AND table_name    = 'events'
        ORDER BY ordinal_position
    """).show(truncate=False)

    print(f"\n-- Schemas in {UC_CATALOG} --")
    spark.sql(f"""
        SELECT catalog_name, schema_name
        FROM system.information_schema.schemata
        WHERE catalog_name = '{UC_CATALOG}'
        ORDER BY schema_name
    """).show(truncate=False)


def demonstrate_data_lineage(spark):
    print("=== Unity Catalog Data Lineage ===")

    print("\n-- Lineage tracking is automatic in Unity Catalog --")
    print("  UC records column-level lineage for all SQL and DataFrame operations.")
    print("  Lineage is viewable in the Databricks UI under the Lineage tab.")

    print("\n-- Example: derived table creates lineage from source --")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.event_summary AS
        SELECT
            event_type,
            COUNT(*)            AS event_count,
            COUNT(DISTINCT user_id) AS unique_users,
            MIN(event_ts)       AS first_event,
            MAX(event_ts)       AS last_event
        FROM {UC_CATALOG}.{UC_SCHEMA}.events
        GROUP BY event_type
    """)
    print(f"  Created {UC_CATALOG}.{UC_SCHEMA}.event_summary")
    print(
        f"  Lineage: {UC_CATALOG}.{UC_SCHEMA}.events → {UC_CATALOG}.{UC_SCHEMA}.event_summary"
    )

    spark.sql(f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.event_summary").show()

    print("\n-- Audit log access (system.access.audit) --")
    print("  Databricks tracks all data access in system.access.audit.")
    print("  Example query:")
    audit_query = f"""
        SELECT event_time, user_identity.email, action_name, request_params.full_name_arg
        FROM system.access.audit
        WHERE action_name IN ('getTable', 'commandSubmit')
          AND request_params.full_name_arg LIKE '{UC_CATALOG}.{UC_SCHEMA}.%'
        ORDER BY event_time DESC
        LIMIT 20
    """
    for line in audit_query.strip().split("\n"):
        print(f"    {line}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    enable_unity_catalog(spark, DBX_WORKSPACE_URL)
    set_catalog_and_schema(spark, UC_CATALOG, UC_SCHEMA)

    try:
        demonstrate_uc_three_level_namespace(spark)
        demonstrate_uc_governance(spark)
        demonstrate_uc_information_schema(spark)
        demonstrate_data_lineage(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
