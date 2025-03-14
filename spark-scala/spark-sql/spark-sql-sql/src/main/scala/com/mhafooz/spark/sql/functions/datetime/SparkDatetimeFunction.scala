package com.mhafooz.spark.sql.functions.datetime

import org.apache.spark.sql.SparkSession

object SparkDatetimeFunction {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDatetimeFunction")
      .getOrCreate()

    val infoDF = spark.sql("select current_date() as today , 1 + 100 as value")
    infoDF.show
  }

}
