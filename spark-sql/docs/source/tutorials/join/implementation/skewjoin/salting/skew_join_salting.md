package com.mahfooz.spark.join.implementation.skewjoin.salting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit, rand}

/**
 * Introduce a random component into the key to distribute skewed data more evenly. This is known as salting.
 * For example, you could add a random number to the key during the pre-processing stage.
 */
object SkewJoinSlating {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SkewJoinRepartition")
      .getOrCreate()

    // Example DataFrames
    val skewedData = spark.read.csv("path/to/data1.csv").toDF("joinKey", "value1")// load skewed data

    // Add a random suffix to the key to reduce skew
    val saltedData = skewedData.withColumn("saltedKey", concat(col("joinKey"), lit("_", rand(42))))
    val result = saltedData.join(otherTable, "saltedKey")
  }
}
