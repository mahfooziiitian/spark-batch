package com.mahfooz.spark.df.writer.parquet

import org.apache.spark.sql.SparkSession

object ParquetWriter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ParquetSpark")
      .master("local")
      .getOrCreate()

    val parquetFile=spark.read.format("parquet")
      .load("/2010-summary.parquet")

    parquetFile.write.format("parquet")
      .mode("overwrite")
      .save("/tmp/my-parquet-file.parquet")
  }
}
