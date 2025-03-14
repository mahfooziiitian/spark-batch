package com.mahfooz.sparksql.generic.view.local

import org.apache.spark.sql.SparkSession

object LocalTempView {

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
    df.select("name").show()

    //Running SQL Queries Programmatically
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

  }
}
