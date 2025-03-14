package com.mahfooz.spark.excel.workbook.reader.df.schema

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataFrameCustomSchema {

  val dataHome = sys.env.getOrElse("DATA_HOME","data")
  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val excelInputFile = s"${dataHome}/spark/datasource/excel/people.xlsx"
    val excelOutputFile = s"${dataHome}/spark/datasource/excel/people_spark.xlsx"

    val peopleSchema = StructType(Array(
      StructField("Name", StringType, nullable = false),
      StructField("Age", IntegerType, nullable = false),
      StructField("Occupation", StringType, nullable = false),
      StructField("Date of birth", DateType, nullable = false)))

    val spark: SparkSession = SparkSession.builder()
      .appName("dataframe-reader-custom-schema")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("com.crealytics.spark.excel")
      //.option("sheetName", "Info")
      .option("header", "true")
      .schema(peopleSchema)
      .load(excelInputFile)

    df.printSchema()
    df.show()
  }
}
