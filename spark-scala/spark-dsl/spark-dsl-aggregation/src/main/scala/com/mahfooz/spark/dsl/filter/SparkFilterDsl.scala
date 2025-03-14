package com.mahfooz.spark.dsl.filter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkFilterDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkFilterDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    import spark.implicits._

    movies.filter('produced_year < 2000).show()
    movies.where('produced_year > 2000).show()

    movies.filter('produced_year >= 2000).show()
    movies.where('produced_year >= 2000).show()

    // equality comparison require 3 equal signs
    movies.filter('produced_year === 2000).show(5)

    movies
      .select("movie_title", "produced_year")
      .filter('produced_year =!= 2000)
      .show(5)

    movies.filter('produced_year >= 2000 && length('movie_title) < 5).show(5)

    spark.stop()
  }
}
