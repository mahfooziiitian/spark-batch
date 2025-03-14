package com.mahfooz.dataframe.columns.sort

import org.apache.spark.sql.SparkSession

object SparkOrderByDsl {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName(SparkOrderByDsl.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    val movieTitles = movies
      .dropDuplicates("movie_title")
      .selectExpr("movie_title", "length(movie_title) as title_length","produced_year")

    import spark.implicits._

    // sorting in descending order
    movieTitles.orderBy('title_length.desc).show(5)

    // sorting by two columns in different orders
    movieTitles.orderBy('title_length.desc, 'produced_year).show(5)

  }
}
