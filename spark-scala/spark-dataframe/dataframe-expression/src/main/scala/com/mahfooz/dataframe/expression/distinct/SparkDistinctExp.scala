package com.mahfooz.dataframe.expression.distinct

import org.apache.spark.sql.SparkSession

object SparkDistinctExp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkDistinctExp.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    movies
      .select("movie_title")
      .distinct
      .selectExpr("count(movie_title) as movies")
      .show

    movies
      .dropDuplicates("movie_title")
      .selectExpr("count(movie_title) as movies")
      .show

  }
}
