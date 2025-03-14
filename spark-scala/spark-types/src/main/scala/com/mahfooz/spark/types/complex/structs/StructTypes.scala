/*

You can think of structs as DataFrames within DataFrames.

 */
package com.mahfooz.spark.types.complex.structs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, col}

object StructTypes {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StructTypes")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    val complexDF = df.select(
      struct("Description", "InvoiceNo")
        .alias("complex")
    )

    df.selectExpr("(Description, InvoiceNo) as complex", "*").show()

    df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show()

    complexDF.select("complex.Description").show()

    complexDF.select(col("complex").getField("Description"))

    complexDF.select("complex.*").show()

  }
}
