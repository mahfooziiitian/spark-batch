package com.mahfooz.spark.types.numbers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, pow}
import org.apache.spark.sql.functions.{round, bround,lit}

object SparkNumbers {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SparkNumbers")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"),
      fabricatedQuantity.alias("realQuantity")).show(2)


    df.selectExpr(  "CustomerId",  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

    df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
  }
}
