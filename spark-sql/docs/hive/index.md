# Hive Integration

Spark SQL integrates with Apache Hive to access the Hive Metastore, read and write Hive tables,
and execute Hive UDFs. This enables seamless interoperability with existing Hive workloads.

---

## 📌 Syntax — Enable Hive Support

=== "Python"

    ```python
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("HiveApp")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    ```

=== "SQL"

    ```sql
    -- Verify catalog implementation
    SET spark.sql.catalogImplementation = hive;

    -- Confirm active catalog
    SHOW DATABASES;
    ```

=== "spark-submit"

    ```bash
    spark-submit \
      --conf spark.sql.catalogImplementation=hive \
      --conf spark.sql.warehouse.dir=/user/hive/warehouse \
      --conf hive.metastore.uris=thrift://metastore-host:9083 \
      my_app.py
    ```

---

## 🔍 Behavior

### HiveContext vs SparkSession

| Feature | `HiveContext` (Legacy) | `SparkSession` (Modern) |
|---------|------------------------|--------------------------|
| Introduced | Spark 1.x | Spark 2.0+ |
| Hive support | Built-in | `.enableHiveSupport()` |
| SQL dialect | HiveQL | SQL + HiveQL subset |
| Metastore | Embedded Derby / Remote | Embedded Derby / Remote |
| Status | **Deprecated** | ✅ Recommended |
| UDF support | Yes | Yes |
| DataFrames | Yes | Yes |

### Key Capabilities

| Capability | Description |
|------------|-------------|
| Hive Metastore | Persistent table metadata shared across sessions |
| Managed tables | Data lifecycle managed by Spark/Hive |
| External tables | Data at user-defined location; metadata only dropped |
| Partitioning | Hive-style partitioned table support |
| Bucketing | Hash-based bucketing for join optimisation |
| Hive UDFs | Access all built-in and custom Hive functions |
| SerDe support | Custom serialization/deserialization |
| Dynamic partitions | Auto-detect partition values from data |

---

## 🧪 Practical Examples

### List databases and tables

```sql
SHOW DATABASES;
SHOW TABLES IN default;
SHOW TABLES IN analytics LIKE 'sales_*';
```

### Create and query a Hive table

```sql
CREATE TABLE IF NOT EXISTS default.orders (
    order_id   BIGINT,
    customer   STRING,
    amount     DOUBLE,
    order_date DATE
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/orders';

INSERT INTO default.orders VALUES (1, 'Alice', 250.0, '2024-01-15');

SELECT customer, SUM(amount) AS total
FROM default.orders
GROUP BY customer
ORDER BY total DESC;
```

### Quick setup snippet (PySpark)

```python
spark.sql("SHOW DATABASES").show()
spark.sql("USE analytics")
spark.sql("SHOW TABLES").show()
df = spark.sql("SELECT * FROM sales WHERE region = 'EMEA'")
df.show()
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Shared Hive metadata across teams | Enable Hive support |
| Migrating legacy Hive workloads | Use Hive catalog, migrate incrementally |
| Modern data lakehouse | Prefer Unity Catalog / Iceberg if available |
| Ad-hoc Spark-only workloads | `in-memory` catalog is sufficient |
| Cross-platform table sharing | Hive Metastore as common catalog |
