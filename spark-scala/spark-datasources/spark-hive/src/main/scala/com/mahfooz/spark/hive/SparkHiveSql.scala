/*

export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export SPARK_DIST_CLASSPATH=$(/path/to/hadoop/bin/hadoop classpath)
export SPARK_DIST_CLASSPATH=$(hadoop --config /path/to/configs classpath)

 */
package com.mahfooz.spark.hive

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(key: Int, value: String)

object SparkHiveSql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql(
      "CREATE TABLE IF NOT EXISTS tutorial.src (key INT, value STRING) USING hive"
    )
    sql(
      "LOAD DATA INPATH 'hdfs://mtmdevhdopms01.intl.vsnl.co.in:8020/user/hdfs/malam/kv1.txt' INTO TABLE tutorial.src"
    )

    // Queries are expressed in HiveQL
    sql("SELECT * FROM tutorial.src").show()

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM tutorial.src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql(
      "SELECT key, value FROM tutorial.src WHERE key < 10 ORDER BY key"
    )

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    stringsDS.show()

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF =
      spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN tutorial.src s ON r.key = s.key").show()

    // Create a Hive managed Parquet table, with HQL syntax instead of the Spark SQL native syntax
    // `USING hive`
    sql(
      "CREATE TABLE tutorial.hive_records(key int, value string) STORED AS PARQUET"
    )

    // Save DataFrame to the Hive managed table
    val df = spark.table("tutorial.src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("tutorial.hive_records")

    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()

    // Prepare a Parquet data directory
    val dataDir = "/tmp/parquet_data"

    spark.range(10).write.parquet(dataDir)

    // Create a Hive external Parquet table
    sql(
      s"CREATE EXTERNAL TABLE tutorial.hive_bigints(id bigint) STORED AS PARQUET LOCATION '$dataDir'"
    )

    // The Hive external table should already have data
    sql("SELECT * FROM tutorial.hive_bigints").show()

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // Create a Hive partitioned table using DataFrame API
    df.write
      .partitionBy("key")
      .format("hive")
      .saveAsTable("tutorial.hive_part_tbl")

    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM tutorial.hive_part_tbl").show()
    spark.stop()
  }
}
