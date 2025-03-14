package com.mahfooz.spark.excel.workbook.reader.df.sheet

import com.crealytics.spark.excel.{ExcelDataFrameReader, WorkbookReader}
import org.apache.spark.sql.SparkSession

object DataFrameReaderExcelSheet {

  def main(args: Array[String]): Unit = {

    val excelFilePath=args(0)

    val spark: SparkSession = SparkSession.builder()
      .appName("dataframe-reader-excel-sheet")
      .master("local")
      .getOrCreate()

    val sheetNames = WorkbookReader(Map("path" -> excelFilePath), spark.sparkContext.hadoopConfiguration).sheetNames
    println(sheetNames(0))
    val df = spark.read.excel(
      header = true,
      dataAddress = sheetNames(0)
    ).load(excelFilePath)

    df.printSchema()

  }
}
