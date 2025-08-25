package com.mhafooz.spark.sql.view.global

import org.apache.spark.sql.SparkSession

object SparkGlobalView {

  def main(args: Array[String]): Unit = {

    val sourceFileName = "D:\\data\\flight-data\\parquet\\2010-summary.parquet"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkGlobalView")
      .getOrCreate()

    val movies = spark.read.parquet(sourceFileName);

    movies.createOrReplaceGlobalTempView("movies_g")

    spark.catalog.listTables().show()

    spark.sql("select count(*) from global_temp.movies_g").show

    spark.stop()
  }
}
