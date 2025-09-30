# Delta Lake Metastore (via Hive or Unity Catalog)

Delta Lake tables can be registered in either the Hive Metastore or Databricks Unity Catalog, depending on your environment and governance needs.

- **Hive Metastore**: The default choice for open-source Delta Lake deployments. It provides basic table registration and metadata management.
- **Unity Catalog**: Available in Databricks environments, Unity Catalog offers centralized governance, fine-grained access control, and lineage tracking for data assets.

Choosing between Hive Metastore and Unity Catalog depends on your requirements for security, compliance, and data management features.
