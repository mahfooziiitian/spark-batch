# Metastore
Metastore (aka metastore_db) is a relational database that is used by Hive, Presto, Spark, etc. to manage the metadata of persistent relational entities (e.g. databases, tables, columns, partitions) for fast access.

    hive.metastore.uris

Spark SQL by default uses an In-Memory catalog/metastore deployed with Apache Derby database.

# Spark Warehouse

A spark-warehouse is the directory where Spark SQL persists tables.

    spark.sql.warehouse.dir

# Enable Hive metastore

To enable Hive Metastore, we need to add .enableHiveSupport() API when we create our Spark Session object and is by default deployed with Apache Derby database, but we can change to any other database like MySQL.

    println(spark.sharedState.externalCatalog.unwrapped)


