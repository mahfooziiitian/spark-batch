package com.mahfooz.spark.sdv.parquet

import com.mahfooz.spark.sdv.model.Flight
import org.apache.spark.sql.{Encoders, SparkSession}

object ReadParquet {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReadParquet")
      .master("local")
      .getOrCreate()

    val flightDF = spark.read
      .parquet(getClass.getResource("/2010-summary.parquet").getFile)

  }
}
