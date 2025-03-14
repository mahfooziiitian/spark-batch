# Enabling Hive Support- enableHiveSupport Method
    
    enableHiveSupport(): Builder

enableHiveSupport enables Hive support (that allows running structured queries on Hive tables, a persistent Hive metastore, support for Hive serdes and user-defined functions).


You do not need any existing Hive installation to use Spark’s Hive support.

SparkSession context will automatically create metastore_db in the current directory of 
a Spark application and the directory configured by spark.sql.warehouse.dir 
configuration property.

Internally, enableHiveSupport checks whether the Hive classes are available or not. If so, enableHiveSupport sets spark.sql.catalogImplementation internal configuration property to hive. Otherwise, enableHiveSupport throws an IllegalArgumentException:
