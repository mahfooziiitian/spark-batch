package com.mahfooz.spark.join.implementation.skewjoin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 *Ensure that the data is evenly distributed across partitions. This may involve choosing a good partitioning key or using a more sophisticated partitioning strategy.
 * If possible, use a partitioning key that evenly distributes the data. For example, in Spark, the repartition or coalesce operations can be used to redistribute the data.
 */
object SkewJoinRepartition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SkewJoinRepartition")
      .getOrCreate()

    val numPartitions =5

    // Example DataFrames
    val data = spark.read.csv("path/to/data1.csv").toDF("joinKey", "value1")
    val skewedKeys = Seq("key1")

    val skewedData = data.filter(col("joinKey").isin(skewedKeys: _*))
    val nonSkewedData = data.filter(!col("joinKey").isin(skewedKeys: _*))

    val repartitionedSkewedData = skewedData.repartition(numPartitions, col("joinKey"))
    val result = repartitionedSkewedData.join(nonSkewedData, "joinKey")
  }
}
