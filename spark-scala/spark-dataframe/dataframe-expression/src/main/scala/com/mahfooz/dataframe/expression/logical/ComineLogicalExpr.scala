package com.mahfooz.dataframe.expression.logical

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComineLogicalExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(ComineLogicalExpr.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    import spark.implicits._

    // to combine one or more comparison expressions, we will use either the OR and AND expression operator
      movies.filter('produced_year >= 2000 && length('movie_title) < 5).show(5)

    // the other way of accomplishing the same result is by calling the filter function two times
    movies.filter('produced_year >= 2000).filter(length('movie_title) < 5).show(5)
  }
}
