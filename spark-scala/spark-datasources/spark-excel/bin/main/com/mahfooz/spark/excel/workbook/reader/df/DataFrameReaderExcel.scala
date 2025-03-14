package com.mahfooz.spark.excel.workbook.reader.df

import org.apache.spark.sql.SparkSession

object DataFrameReaderExcel {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("dataframe-reader").master("local").getOrCreate()

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      .option("header", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("setErrorCellsToFallbackValues", "true") // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
      .option("usePlainNumberFormat", "false") // Optional, default: false, If true, format the cells without rounding and scientific notations
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "true") // Optional, default: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
      //.schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load("Worktime.xlsx")
  }
}
