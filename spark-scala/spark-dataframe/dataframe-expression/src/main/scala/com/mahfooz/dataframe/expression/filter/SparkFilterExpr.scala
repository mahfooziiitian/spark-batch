package com.mahfooz.dataframe.expression.filter

import org.apache.spark.sql.SparkSession

object SparkFilterExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    import spark.implicits._

    movies.filter('produced_year < 2005)
      .show()

    movies.where('produced_year > 2005)
      .show()

  }

}
