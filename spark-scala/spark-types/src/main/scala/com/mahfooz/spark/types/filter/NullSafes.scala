package com.mahfooz.spark.types.filter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object NullSafes {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("NullSafes")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.where(col("Description").eqNullSafe("ASSORTED COLOUR BIRD ORNAMENT")).show()

  }
}
