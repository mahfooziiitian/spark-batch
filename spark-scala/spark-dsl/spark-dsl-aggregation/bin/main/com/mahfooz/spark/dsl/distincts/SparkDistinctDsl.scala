package com.mahfooz.spark.dsl.distincts

import org.apache.spark.sql.SparkSession

object SparkDistinctDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDistinctDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    movies
      .select("movie_title")
      .distinct
      .selectExpr("count(movie_title) as movies")
      .show

    spark.stop()
  }
}
