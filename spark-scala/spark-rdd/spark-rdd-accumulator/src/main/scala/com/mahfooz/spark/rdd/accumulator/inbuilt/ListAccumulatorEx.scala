package com.mahfooz.spark.rdd.accumulator.inbuilt

import org.apache.spark.sql.SparkSession

object ListAccumulatorEx {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("ListAccumulatorEx")
      .getOrCreate()


  }

}
