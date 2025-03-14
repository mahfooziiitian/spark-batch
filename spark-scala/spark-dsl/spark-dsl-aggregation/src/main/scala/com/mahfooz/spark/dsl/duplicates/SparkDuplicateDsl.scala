package com.mahfooz.spark.dsl.duplicates

import org.apache.spark.sql.SparkSession

object SparkDuplicateDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDuplicateDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    movies
      .dropDuplicates("movie_title")
      .selectExpr("count(movie_title) as movies")
      .show

    spark.stop()
  }
}
