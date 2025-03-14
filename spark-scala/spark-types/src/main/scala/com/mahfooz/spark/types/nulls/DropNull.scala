package com.mahfooz.spark.types.nulls

import com.mahfooz.spark.types.nulls.Coalesce.getClass
import org.apache.spark.sql.SparkSession

object DropNull {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("DropNull")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.na.drop()
    df.na.drop("any")

  }
}
