package com.mahfooz.spark.rdd.datasource.pair

import org.apache.spark.sql.SparkSession

object ByKeys {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ByKeys")
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(
      List("Spark", "is", "an", "amazing", "piece", "of", "technology")
    )

    val keyword = rdd.keyBy(word => word.toLowerCase.toSeq(0).toString)
  }
}
