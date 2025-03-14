package com.mahfooz.dataframe.expression.count

import org.apache.spark.sql.SparkSession

object SparkCountExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    movies
      .selectExpr(
        "count(distinct(movie_title)) as movies",
        "count(distinct(actor_name)) as actors"
      )
      .show
  }
}
