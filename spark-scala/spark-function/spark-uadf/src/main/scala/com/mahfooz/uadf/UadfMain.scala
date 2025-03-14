package com.mahfooz.uadf

import org.apache.spark.sql.SparkSession

object UadfMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("UadfMain")
      .getOrCreate()

    val ba=new BoolAnd
    //val ba = spark.udf.register("booland", ba)
  }
}
