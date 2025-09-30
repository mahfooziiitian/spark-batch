# Warehouse

The spark.sql.warehouse.dir configuration in Spark specifies the default location for the Spark SQL warehouse, which is where managed tables are stored when using the Hive catalog or enabling Hive support.

spark.sql.warehouse.dir: Directory for managed tables.

## What It Does

When you create a managed table in Spark SQL (i.e., without specifying a location), Spark stores the data in the directory defined by:

```text
spark.sql.warehouse.dir
```
