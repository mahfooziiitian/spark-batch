package com.mahfooz.spark.config

import org.apache.spark.sql.SparkSession

object SqlConfig {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")
    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "").replace("\\", "/")
    )

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHiveDatabase")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    println(s"Broadcast timeout : ${spark.conf.get("spark.sql.broadcastTimeout")}")
    println(s"Broadcast threshold : ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")
    println(s"Shuffle partition : ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"InMemory compression : ${spark.conf.get("spark.sql.inMemoryColumnarStorage.compressed")}")
    println(s"InMemory batch size : ${spark.conf.get("spark.sql.inMemoryColumnarStorage.batchSize")}")
    println(s"File max partition bytes: ${spark.conf.get("spark.sql.files.maxPartitionBytes")}")
    println(s"File max open cost bytes: ${spark.conf.get("spark.sql.files.openCostInBytes")}")

  }

}
