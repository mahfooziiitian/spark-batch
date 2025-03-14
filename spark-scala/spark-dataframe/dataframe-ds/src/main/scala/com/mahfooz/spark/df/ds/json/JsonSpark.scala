package com.mahfooz.spark.df.ds.json

import org.apache.spark.sql.SparkSession

object JsonSpark {

  def main(args: Array[String]): Unit = {


    val PATH_PREFIX="D:\\data\\flight-data\\json"

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .load(PATH_PREFIX+"\\2015-summary.json")

    df.printSchema()

    spark.read
      .option("mode", "FAILFAST")
      .json(PATH_PREFIX+"\\2015-summary.json")
      .show(5)
  }
}
