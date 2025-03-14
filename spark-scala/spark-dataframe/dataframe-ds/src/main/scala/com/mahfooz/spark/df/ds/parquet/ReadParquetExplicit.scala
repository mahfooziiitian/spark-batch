package com.mahfooz.spark.df.ds.parquet

import org.apache.spark.sql.SparkSession

object ReadParquetExplicit {

  def main(args: Array[String]): Unit = {

    val PREFIX_PATH="D:\\data\\flight-data\\parquet"

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    spark.read.format("parquet")
      .load(PREFIX_PATH+"\\2010-summary.parquet")
      .show(5)
  }
}
