package com.mahfooz.sparksql.generic.view.global

import org.apache.spark.sql.SparkSession

object GlobalTempView {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

    val df = spark.read
      .format("json")
      .load(args(0))

    // Displays the content of the DataFrame to stdout
    df.show()
    df.printSchema()

    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark
      .sql("SELECT * FROM global_temp.people")
      .show()

    // Global temporary view is cross-session
    spark
      .newSession()
      .sql("SELECT * FROM global_temp.people")
      .show()
  }
}
