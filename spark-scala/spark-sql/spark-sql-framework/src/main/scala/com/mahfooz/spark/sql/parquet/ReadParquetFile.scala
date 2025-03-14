package com.mahfooz.spark.sql.parquet

import org.apache.spark.sql.SparkSession

object ReadParquetFile {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("SparkGlobalView")
      .master("local[*]")
      .getOrCreate()

    spark.sql("SELECT * FROM parquet.`D:/data/movies/movies.parquet`").show(5)
  }
}
