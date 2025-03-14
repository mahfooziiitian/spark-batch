package com.mahfooz.dataframe.columns.`type`

import org.apache.spark.sql.SparkSession

object ColumnTypeCast {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load(getClass.getResource("/json/2015-summary.json").getFile)

    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
      .columns
      .foreach(println)

  }
}
