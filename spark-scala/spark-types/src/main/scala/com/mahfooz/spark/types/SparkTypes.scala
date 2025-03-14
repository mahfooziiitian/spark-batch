package com.mahfooz.spark.types

import org.apache.spark.sql.SparkSession

object SparkTypes {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SparkTypes")
      .master("local")
      .getOrCreate()

    // in Scala
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)
    df.printSchema()
    df.createOrReplaceTempView("dfTable")
  }
}
