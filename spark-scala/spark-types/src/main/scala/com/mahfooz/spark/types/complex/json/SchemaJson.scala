package com.mahfooz.spark.types.complex.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{to_json,col,from_json}

object SchemaJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SchemaJson")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    val parseSchema = new StructType(Array(
      new StructField("InvoiceNo",StringType,true),
      new StructField("Description",StringType,true)))

    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("newJSON"))
      .select(from_json(col("newJSON"), parseSchema), col("newJSON"))
      .show(2)

  }
}
