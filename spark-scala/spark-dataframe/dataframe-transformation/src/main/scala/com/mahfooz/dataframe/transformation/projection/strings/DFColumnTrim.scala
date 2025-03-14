package com.mahfooz.dataframe.transformation.projection.strings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, lpad, ltrim, rpad, rtrim, trim}

object DFColumnTrim {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DFColumnTrim")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    /*
    SELECT
      ltrim('    HELLLOOOO  '),
      rtrim('    HELLLOOOO  '),
      trim('    HELLLOOOO  '),
      lpad('HELLOOOO  ', 3, ' '),
      rpad('HELLOOOO  ', 10, ' ')
    FROM dfTable
     */

    df.select(
        ltrim(lit("    HELLO    ")).as("ltrim"),
        rtrim(lit("    HELLO    ")).as("rtrim"),
        trim(lit("    HELLO    ")).as("trim"),
        lpad(lit("HELLO"), 3, " ").as("lp"),
        rpad(lit("HELLO"), 10, " ").as("rp")
      )
      .show(2)
  }

}
