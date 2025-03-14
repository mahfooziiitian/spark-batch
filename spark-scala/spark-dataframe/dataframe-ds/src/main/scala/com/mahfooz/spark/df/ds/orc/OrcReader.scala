/*
Optimized Row Columnar (ORC) is another popular open source self-describing columnar storage format in the Hadoop ecosystem.

It was created by a company called Cloudera as part of the initiative to massively speed up Hive.

It is quite similar to Parquet in terms of efficiency and speed and was designed for analytics workloads.

Working with  ORC files is just as easy as working with Parquet files.


 */
package com.mahfooz.spark.df.ds.orc

import org.apache.spark.sql.SparkSession

object OrcReader {
  def main(args: Array[String]): Unit = {

    val PREFIX_PATH = "D:\\data\\flight-data\\orc"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("OrcReader")
      .getOrCreate()

    spark.read
      .format("orc")
      .load(PREFIX_PATH + "\\2010-summary.orc")
      .show(5)

  }
}
