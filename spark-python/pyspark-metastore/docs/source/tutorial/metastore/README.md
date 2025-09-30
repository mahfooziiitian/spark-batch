# Metastore

## Configuration Comparison Table

| Catalog Type | Configuration Class   | Use Case                   | Persistence |
|--------------|----------------------|----------------------------|-------------|
| Hive         | `HiveCatalog`        | Traditional data lakes     | High        |
| Unity        | `UnityCatalog`       | Enterprise governance      | High        |
| Iceberg      | `SparkCatalog`       | ACID transactions          | High        |
| Glue         | `AWSGlueCatalog`     | AWS ecosystems             | High        |
| JDBC         | `JDBCTableCatalog`   | RDBMS integration          | Medium      |
| In-Memory    | `InMemoryCatalog`    | Development                | None        |

> **Tip:** Choose the catalog type based on your data governance, persistence, and integration needs.
