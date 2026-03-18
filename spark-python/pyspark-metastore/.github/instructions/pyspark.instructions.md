---
applyTo: "src/**/*.py"
---

# PySpark Metastore Code Instructions

## SparkSession — Catalog-Aware Pattern

Every metastore script must configure the catalog explicitly.
Use `enableHiveSupport()` only when connecting to a Hive-compatible metastore.

```python
import os
from pyspark.sql import SparkSession

warehouse_dir = os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse")

spark = (SparkSession.builder
         .appName("metastore-job")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.warehouse.dir", warehouse_dir)
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

## Catalog Registration

Register catalogs via `spark.sql.catalog.<name>` configs. Use named catalogs — never
overwrite `spark_catalog` unless replacing the built-in with Delta or Iceberg.

```python
# Iceberg catalog
.config("spark.sql.catalog.my_iceberg", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.my_iceberg.type", "hive")
.config("spark.sql.catalog.my_iceberg.uri", "thrift://metastore:9083")
.config("spark.sql.catalog.my_iceberg.warehouse", "s3://bucket/iceberg")

# Delta Lake catalog (replaces built-in)
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

# JDBC catalog
.config("spark.sql.catalog.jdbc", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
.config("spark.sql.catalog.jdbc.url", jdbc_url)
.config("spark.sql.catalog.jdbc.driver", "org.postgresql.Driver")

# Glue catalog
.config("spark.sql.catalogImplementation", "hive")
.config("spark.hadoop.hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")

# REST catalog (Iceberg)
.config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.rest.type", "rest")
.config("spark.sql.catalog.rest.uri", "https://metastore-api.example.com")
```

## Three-Level Namespace Resolution

Always reference tables using the three-level `catalog.database.table` format in examples:

```python
spark.sql("SELECT * FROM spark_catalog.default.my_table")   # fully qualified
spark.sql("SELECT * FROM default.my_table")                  # default catalog
spark.sql("SELECT * FROM my_table")                          # current catalog + db
```

## Catalog Introspection Helper

Reuse the `catalog_metadata` module for listing catalogs, databases, and tables:

```python
from metastore.catalog_metadata import print_catalog_metadata

metadata = print_catalog_metadata(spark)
```

When writing new introspection functions, follow the pattern in
`src/metastore/catalog_metadata.py` — accept `spark: SparkSession` as the first arg
and return dicts or DataFrames, not printed strings.

## Credentials & Sensitive Config

Never hard-code credentials. Always load from environment variables:

```python
jdbc_user     = os.environ.get("JDBC_USER", "username")
jdbc_password = os.environ.get("JDBC_PASSWORD", "password")
jdbc_url      = os.environ.get("JDBC_URL", "jdbc:postgresql://localhost:5432/metastore")
```

For AWS, rely on the default credential provider chain:
```python
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
```

## JAVA_HOME

Set `JAVA_HOME` from env vars at the top of scripts that need it:

```python
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_11", "")
```

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
```

Never use `from pyspark.sql.functions import *`.

## Script Structure

Each metastore sub-module script should:
1. Configure the SparkSession with the relevant catalog.
2. Demonstrate `SHOW CATALOGS` / `SHOW DATABASES` / `SHOW TABLES`.
3. Create a sample table, query it, and drop it.
4. Call `spark.stop()` at the end.

```python
if __name__ == "__main__":
    main()
    spark.stop()
```
