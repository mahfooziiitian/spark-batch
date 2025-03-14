package com.mahfooz.dataframe.columns.sort

import org.apache.spark.sql.SparkSession

object SparkSortDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkSortDsl.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    val movieTitles = movies
      .dropDuplicates("movie_title")
      .selectExpr("movie_title", "length(movie_title) as title_length","produced_year")

    import spark.implicits._

    movieTitles.sort('title_length).show(20)

  }
}
