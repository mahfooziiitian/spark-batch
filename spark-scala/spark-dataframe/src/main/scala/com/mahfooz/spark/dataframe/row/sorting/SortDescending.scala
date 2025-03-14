package com.mahfooz.spark.dataframe.row.sorting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, asc,expr}

object SortDescending {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SortDescending")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("json")
      .load(getClass.getResource("/2015-summary.json").getFile)

    df.orderBy(expr("count desc")).show(2)
    df.orderBy(desc("count"),
      asc("DEST_COUNTRY_NAME")).show(2)

  }
}
