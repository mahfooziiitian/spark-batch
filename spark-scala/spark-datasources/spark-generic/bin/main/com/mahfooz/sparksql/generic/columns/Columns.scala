package com.mahfooz.sparksql.generic.columns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Columns {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .format("json")
      .load(args(0))

    df.select(
        df.col("DEST_COUNTRY_NAME"),
        col("DEST_COUNTRY_NAME"),
        column("DEST_COUNTRY_NAME"),
        'DEST_COUNTRY_NAME,
        $"DEST_COUNTRY_NAME",
        expr("DEST_COUNTRY_NAME")
      )
      .show(2)
  }
}
