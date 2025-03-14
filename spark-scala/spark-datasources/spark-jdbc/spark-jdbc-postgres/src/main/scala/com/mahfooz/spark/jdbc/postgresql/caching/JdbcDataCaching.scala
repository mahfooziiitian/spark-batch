/*

Spark SQL can cache tables using an in-memory columnar format by calling

  a)  spark.catalog.cacheTable("tableName")
  b)  dataFrame.cache()

Then Spark SQL will scan only required columns and will automatically tune compression to minimize memory usage and GC pressure.
You can call below method remove the table from memory.
  spark.catalog.uncacheTable("tableName")

Configuration of in-memory caching can be done using the
  a)  setConf method on SparkSession
  b)  By running SET key=value commands using SQL

 */
package com.mahfooz.spark.jdbc.postgresql.caching

class JdbcDataCaching {

}
