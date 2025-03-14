package com.mahfooz.dataframe.expression.where

import org.apache.spark.sql.SparkSession

object SparkWhereExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkWhereExpr.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json");

    import spark.implicits._

    movies.where('produced_year > 2005)
      .show()
  }
}
