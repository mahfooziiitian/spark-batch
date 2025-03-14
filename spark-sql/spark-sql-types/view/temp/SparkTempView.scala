package com.mhafooz.spark.sql.view.temp

import org.apache.spark.sql.SparkSession

object SparkTempView {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkTempView")
      .getOrCreate()

    val movies = spark.read.parquet(args(0));

    // now register movies DataFrame as a temporary view
    movies.createOrReplaceTempView("movies")

    spark.catalog.listTables.show

    spark.catalog.listColumns("movies").show

    spark.stop()
  }
}
