# Spark managed table

When you use saveAsTable on a DataFrame, you are creating a managed table for which Spark
will track of all of the relevant information.

This will read your table and write it out to a new location in Spark format.

You can set this by setting the `spark.sql.warehouse.dir` configuration to the directory of your choosing
when you create your SparkSession.

By default Spark sets this to `/user/hive/warehouse`.
