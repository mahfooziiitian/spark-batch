package com.mahfooz.spark.sqlhive.table.partition.dynamic

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataLoadingDynamicPartition {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    println(sparkWarehouse)

    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    // Save DataFrame to the Hive managed table
    val df = spark.table("src")

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    // Create a Hive partitioned table using DataFrame API
    df.limit(20).write.partitionBy("key")
      .format("hive")
      .mode(SaveMode.Overwrite)
      .saveAsTable("hive_part_tbl")

    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM hive_part_tbl").show()

  }
}
