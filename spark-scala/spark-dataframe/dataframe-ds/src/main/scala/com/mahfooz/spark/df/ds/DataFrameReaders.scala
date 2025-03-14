/*
The foundation for reading data in Spark is the DataFrameReader.
We access this through the SparkSession via the read attribute:
  spark.read

After we have a DataFrame reader, we specify several values:

  a)The format
  b)The schema
  c)The read mode
  d)A series of options
The format, options, and schema each return a DataFrameReader that can undergo further transformations and are
all optional, except for one option.
At a minimum, you must supply the DataFrameReader a path to from which to read.

 */
package com.mahfooz.spark.df.ds

import org.apache.spark.sql.SparkSession

object DataFrameReaders {

  val PREFIX_DATA_PATH="d:\\data"

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
        .appName("DataFrameReaders")
        .master("local")
        .getOrCreate()

    println(PREFIX_DATA_PATH+"\\2010-12-01.csv")

    val df=spark.read.format("csv")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .option("path", PREFIX_DATA_PATH+"\\2010-12-01.csv")
      .load()

    df.show()
  }
}
