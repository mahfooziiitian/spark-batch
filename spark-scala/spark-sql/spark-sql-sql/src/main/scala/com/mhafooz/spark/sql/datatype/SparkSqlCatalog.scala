package com.mhafooz.spark.sql.datatype

import org.apache.spark.sql.SparkSession

object SparkSqlCatalog {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSqlCatalog")
      .getOrCreate()

    spark.catalog.listTables.show

    val movies = spark.read.parquet(
      "D:\\data\\flight-data\\parquet\\2010-summary.parquet"
    );

    // now register movies DataFrame as a temporary view
    movies.createOrReplaceTempView("movies")

    spark.catalog.listTables.show

    // show the list of columns of movies view in catalog
    spark.catalog.listColumns("movies").show

    spark.stop()
  }
}
