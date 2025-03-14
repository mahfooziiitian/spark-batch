package com.mahfooz.spark.types.filter

import com.mahfooz.spark.types.filter.Filters.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{expr, not, col}

object FiltersColumns {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("FiltersColumns")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

  }
}
