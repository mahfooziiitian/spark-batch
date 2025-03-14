# Metastore

Metastore (aka metastore_db) is a relational database that is used by Hive, Presto, Spark, etc. to manage the metadata of persistent relational entities (e.g. databases, tables, columns, partitions) for fast access.

## I do not have Hive setup in my cluster, can I still use the metastore?
If we do not have Hive setup, we can still use the Hive metastore in Spark. When Hive is not configured in “hive-site.xml” of Spark, then Spark automatically creates metastore (metastore_db) in the current directory, deployed with default Apache Derby and also creates a directory configured by spark.sql.warehouse.dir to store the Spark tables, which defaults to the directory spark-warehouse in the current directory that the Spark application is started. 

## Spark warehouse

Additionally, a spark-warehouse is the directory where Spark SQL persists tables.
