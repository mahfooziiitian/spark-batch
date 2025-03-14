package com.mahfooz.spark.types.complex.arrays


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split,size,array_contains,explode}

object ArrayTypes {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ArrayTypes")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(split(col("Description"),
      " ")).show(2)

    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]").show(2)

    df.select(size(split(col("Description"), " "))).show(2)

    df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

    df.withColumn("splitted", split(col("Description"), " "))
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded").show(2)

  }
}
