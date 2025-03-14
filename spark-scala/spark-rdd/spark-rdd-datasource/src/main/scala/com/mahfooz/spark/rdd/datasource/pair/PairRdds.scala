package com.mahfooz.spark.rdd.datasource.pair

import org.apache.spark.sql.SparkSession

object PairRdds {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CollectAction")
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(
      List("Spark", "is", "an", "amazing", "piece", "of", "technology")
    )
    val pairRDD = rdd.map(w => (w.length, w))
    pairRDD.collect().foreach(println)

  }
}
