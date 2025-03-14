package com.mahfooz.spark.dataframe.row.sorting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SortingRows {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SortingRows")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("json")
      .load(getClass.getResource("/2015-summary.json").getFile)

    df.sort("count").show(5)
    df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
  }
}
