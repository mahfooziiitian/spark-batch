package com.mahfooz.spark.hive

import org.apache.spark.sql.SparkSession

object SparkHiveDs {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
  }
}
