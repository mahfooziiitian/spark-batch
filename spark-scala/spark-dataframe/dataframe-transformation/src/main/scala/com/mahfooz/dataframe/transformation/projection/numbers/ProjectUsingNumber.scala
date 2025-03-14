package com.mahfooz.dataframe.transformation.projection.numbers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, pow}

object ProjectUsingNumber {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ProjectUsingNumber")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    /*
    SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
    FROM dfTable
     */

    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity"))
      .show(2)

    df.selectExpr(
      "CustomerId",
      "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
  }

}
