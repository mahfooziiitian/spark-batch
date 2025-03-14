package com.mhafooz.spark.sql.datatype.datetime.date

import org.apache.spark.sql.SparkSession

object DateDifference {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateDifference")
      .getOrCreate()

  }

}
