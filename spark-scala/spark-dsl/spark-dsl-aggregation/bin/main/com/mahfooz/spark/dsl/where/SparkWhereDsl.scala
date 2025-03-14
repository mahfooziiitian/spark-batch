package com.mahfooz.spark.dsl.where

import org.apache.spark.sql.SparkSession

object SparkWhereDsl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkWhereDsl")
      .getOrCreate()

    val movies =
      spark.read.parquet(args(0))

    import spark.implicits._
    movies.where('produced_year > 2000).show()
    movies.where('produced_year >= 2000).show()
    spark.stop()
  }
}
