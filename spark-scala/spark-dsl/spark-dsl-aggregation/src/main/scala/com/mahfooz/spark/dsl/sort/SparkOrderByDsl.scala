package com.mahfooz.spark.dsl.sort

import org.apache.spark.sql.SparkSession

object SparkOrderByDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkOrderByDsl")
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

    // sorting in descending order
    movieTitles.orderBy('title_length.desc).show(5)
    movieTitles.orderBy('title_length.desc, 'produced_year).show(5)

    spark.stop()
  }
}
