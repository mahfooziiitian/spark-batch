package com.mahfooz.dataframe.expression.filter

import org.apache.spark.sql.SparkSession

object SparkNoFilterExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkNoFilterExpr.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    import spark.implicits._

    movies
      .select("movie_title", "produced_year")
      .filter(
        'produced_year =!=
          2000
      )
      .show(5)

  }

}
