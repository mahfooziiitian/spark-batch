# Iceberg Catalogs

Apache Iceberg supports multiple catalog types for managing table metadata in data lakes. Each catalog offers unique features and integration options:

---

## 1. Hive Catalog

- **Backend:** Uses Hive Metastore.
- **Features:** Leverages existing Hive infrastructure for metadata management.
- **Configuration Example:**

    ```python
    spark.sql.catalog.catalog_name = org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.catalog_name.type = hive
    spark.sql.catalog.catalog_name.uri = thrift://localhost:9083
    ```

- **Use Case:** Ideal for environments already using Hive Metastore.

---

## 2. Nessie Catalog

- **Backend:** Nessie server (Git-like versioned catalog).
- **Features:** Supports branching, tagging, and versioning of tables.
- **Configuration Example:**

    ```python
    spark.sql.catalog.catalog_name = org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.catalog_name.type = nessie
    spark.sql.catalog.catalog_name.uri = http://localhost:19120/api/v1
    spark.sql.catalog.catalog_name.ref = main
    ```

- **Use Case:** Enables reproducible data pipelines and multi-environment workflows.

---

## 3. REST Catalog

- **Backend:** REST API service.
- **Features:** Cloud-native, stateless metadata management.
- **Configuration Example:**

    ```python
    spark.sql.catalog.catalog_name = org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.catalog_name.type = rest
    spark.sql.catalog.catalog_name.uri = https://my-rest-catalog/api
    ```

- **Use Case:** Suitable for cloud deployments and microservices architectures.

---

> **Tip:** Choose the catalog type that best fits your infrastructure and workflow requirements. Refer to the [Iceberg documentation](https://iceberg.apache.org/) for detailed setup instructions.
