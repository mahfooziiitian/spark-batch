package com.mahfooz.spark.partition.defaults

import org.apache.spark.sql.SparkSession

object DefaultNumberPartition {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("default-number-partition")
      .master("local[*]")
      .getOrCreate()

    println(spark.sparkContext.defaultParallelism)
    println(spark.sparkContext.defaultMinPartitions)

    println(spark.conf.get("spark.sql.shuffle.partitions"))

    println(spark.conf.get("spark.sql.adaptive.enabled"))
  }

}
