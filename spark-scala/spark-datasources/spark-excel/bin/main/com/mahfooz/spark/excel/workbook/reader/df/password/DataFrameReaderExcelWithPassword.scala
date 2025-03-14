package com.mahfooz.spark.excel.workbook.reader.df.password

import com.crealytics.spark.excel._
import org.apache.spark.sql.SparkSession

object DataFrameReaderExcelWithPassword {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("dataframe-reader-custom-schema")
      .master("local")
      .getOrCreate()

    val df = spark.read.excel(
      header = true, // Required
      dataAddress = "'My Sheet'!B3:C35", // Optional, default: "A1"
      treatEmptyValuesAsNulls = false, // Optional, default: true
      //setErrorCellsToFallbackValues = false, // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
      //usePlainNumberFormat = false,  // Optional, default: false. If true, format the cells without rounding and scientific notations
      inferSchema = false, // Optional, default: false
      addColorColumns = true, // Optional, default: false
      timestampFormat = "MM-dd-yyyy HH:mm:ss", // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      maxRowsInMemory = 20, // Optional, default None. If set, uses a streaming reader which can help with big files
      excerptSize = 10, // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      workbookPassword = "pass" // Optional, default None. Requires unlimited strength JCE for older JVMs
    ) //.schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load("Worktime.xlsx")

    df.printSchema()
    df.show()
  }
}
