package com.mahfooz.spark.excel.workbook.reader.df.schema

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataFrameCustomSchema {

  def main(args: Array[String]): Unit = {

    val peopleSchema = StructType(Array(
      StructField("Name", StringType, nullable = false),
      StructField("Age", IntegerType, nullable = false),
      StructField("Occupation", StringType, nullable = false),
      StructField("Date of birth", DateType, nullable = false)))

    val spark: SparkSession = SparkSession.builder()
      .appName("dataframe-reader-custom-schema")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("com.crealytics.spark.excel")
      //.option("sheetName", "Info")
      .option("header", "true")
      .schema(peopleSchema)
      .load("People.xlsx")

    df.printSchema()
    df.show()
  }
}
