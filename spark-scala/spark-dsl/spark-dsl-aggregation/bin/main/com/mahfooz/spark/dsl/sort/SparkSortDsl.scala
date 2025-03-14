package com.mahfooz.spark.dsl.sort

import org.apache.spark.sql.SparkSession

object SparkSortDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSortDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    val movieTitles = movies
      .dropDuplicates("movie_title")
      .selectExpr(
        "movie_title",
        "length(movie_title) as title_length",
        "produced_year"
      )

    import spark.implicits._

    movieTitles.sort('title_length).show(5)

    spark.stop()
  }
}
