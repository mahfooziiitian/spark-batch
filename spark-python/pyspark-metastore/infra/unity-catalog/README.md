# Unity Catalog Infrastructure

Unity Catalog is a **Databricks-specific** governance solution — it requires a Databricks workspace and cannot be fully replicated locally with Docker.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Account admin access** to configure metastores
- **Databricks CLI** installed and configured

## Setup via Databricks UI

1. Navigate to **Data** in the Databricks workspace sidebar
2. Create a **Metastore** (account admin) and assign it to your workspace
3. Create a **Catalog** (e.g., `demo_catalog`)
4. Create a **Schema** within the catalog (e.g., `demo_catalog.demo_schema`)
5. Grant appropriate permissions to users/groups

## Setup via Databricks CLI

Run the setup script:

```bash
./setup.sh
```

This will verify CLI connectivity and create a demo catalog and schema.

## Spark Configuration

In Databricks Runtime, Unity Catalog is configured automatically. Key configs:

```
spark.sql.catalog.spark_catalog = com.databricks.sql.transaction.tahoe.catalog.DeltaUnityCatalog
spark.databricks.unityCatalog.enabled = true
```

These are pre-set in Databricks clusters — no manual configuration is needed.

## Local Testing with Open-Source Unity Catalog

The [Unity Catalog open-source server](https://www.unitycatalog.io/) can be used for local development and testing:

```bash
# Clone the open-source Unity Catalog repo
git clone https://github.com/unitycatalog/unitycatalog.git
cd unitycatalog

# Start the server
bin/start-uc-server
```

See [unitycatalog.io](https://www.unitycatalog.io/) for full documentation.

## References

- [Databricks Unity Catalog Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)
- [Open-Source Unity Catalog](https://www.unitycatalog.io/)
