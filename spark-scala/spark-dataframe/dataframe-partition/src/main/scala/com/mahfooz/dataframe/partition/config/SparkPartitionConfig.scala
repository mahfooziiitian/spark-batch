/*
Spark configuration

1)  spark.default.parallelism
    This configuration default value set to the number of all cores on all nodes in a cluster, on local it is set to
    a number of cores on your system.

2)  spark.sql.shuffle.partitions
    This configuration default value is set to 200 and it is used when you call shuffle operations
    like union(),groupBy(),join() and many more.
    This property is available only in DataFrame API but not in RDD.

You can change the values of these properties through programmatically using the below statement.

  spark.conf.set("spark.sql.shuffle.partitions", "500")

You can also set the partition value of these configurations using spark-submit command.

  ./bin/spark-submit \
    --conf spark.sql.shuffle.partitions=500 \
    --conf spark.default.parallelism=500

 */
package com.mahfooz.dataframe.partition.config

import org.apache.spark.sql.SparkSession

object SparkPartitionConfig {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkPartitionConfig")
      .master("local[*]")
      .getOrCreate()

    //println(s"Spark default parallelism: ${spark.conf.get("spark.default.parallelism")}")

    println(s"Spark shuffle partition: ${spark.conf.get("spark.sql.shuffle.partitions")}")


  }
}
