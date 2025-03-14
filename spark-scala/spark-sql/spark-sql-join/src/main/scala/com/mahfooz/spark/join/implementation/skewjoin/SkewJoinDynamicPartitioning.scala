package com.mahfooz.spark.join.implementation.skewjoin

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Dynamic Repartitioning:
 *
 * Use dynamic repartitioning based on run-time statistics. Some frameworks allow you to dynamically adjust the number of partitions based on the distribution of data during runtime.
 */
object SkewJoinDynamicPartitioning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SkewJoinExample")
      .getOrCreate()

    // Example DataFrames
    val df1 = spark.read.csv("path/to/data1.csv").toDF("joinKey", "value1")
    val df2 = spark.read.csv("path/to/data2.csv").toDF("joinKey", "value2")

    // Define the key for join
    val joinKey = "joinKey"

    // Perform dynamic repartitioning based on observed skew
    val skewedKeys = findSkewedKeys(df1, joinKey)

    // Repartition the DataFrames dynamically
    val repartitionedDf1 = dynamicallyRepartition(df1, joinKey, skewedKeys.size)
    val repartitionedDf2 = dynamicallyRepartition(df2, joinKey, skewedKeys.size)

    // Perform the join on the repartitioned DataFrames
    val result = repartitionedDf1.join(repartitionedDf2, joinKey)

    // Continue with your processing on the 'result' DataFrame

    spark.stop()
  }

  private def findSkewedKeys(df: DataFrame, joinKey: String): Set[Any] = {
    // Logic to identify skewed keys based on data distribution analysis
    // For simplicity, assume you have a skew detection function that returns skewed keys
    // Replace this with your actual logic to detect skewed keys

    // For example, consider keys with a count greater than a threshold as skewed
    val skewThreshold = 1000
    df.groupBy(joinKey).count().filter(s"count > $skewThreshold").select(joinKey).collect().toSet
  }

  private def dynamicallyRepartition(df: DataFrame, joinKey: String, numPartitions: Int): DataFrame = {
    // Dynamically repartition DataFrame based on observed skew
    val repartitionedDf = if (numPartitions > 0) {
      df.repartition(numPartitions, df(joinKey))
    } else {
      df // No repartitioning if numPartitions is 0
    }

    repartitionedDf
  }
}
