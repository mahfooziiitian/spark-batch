package com.mahfooz.spark.dataframe.row

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object RowFilters {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("RowFilters")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("json")
      .load(getClass.getResource("/2015-summary.json").getFile)

    df.filter(col("count") < 2).show(2)
    df.where("count < 2").show(2)

    df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)

  }
}
