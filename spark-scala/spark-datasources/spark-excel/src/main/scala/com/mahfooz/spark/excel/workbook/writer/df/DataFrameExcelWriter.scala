package com.mahfooz.spark.excel.workbook.writer.df

import org.apache.spark.sql._

class DataFrameExcelWriter {

  def main(args: Array[String]): Unit = {

    val df: DataFrame = ???
    df.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet'!B3:C35")
      .option("header", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // Optional, default: overwrite.
      .save("Worktime2.xlsx")
  }
}
