package com.mahfooz.spark.dataframe.row.unique

import org.apache.spark.sql.SparkSession

object UniqueRows {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load(getClass.getResource("/json/2015-summary.json").getFile)

    println(df.select("ORIGIN_COUNTRY_NAME").distinct().count())

    val rowsCount = df
      .select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
      .distinct()
      .count()

    println(rowsCount)
  }
}
