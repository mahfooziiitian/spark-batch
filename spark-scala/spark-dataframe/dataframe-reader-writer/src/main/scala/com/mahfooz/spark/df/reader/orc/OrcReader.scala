/*
Optimized Row Columnar (ORC) is another popular open source self-describing columnar storage format in the Hadoop ecosystem.

 */
package com.mahfooz.spark.df.reader.orc

import org.apache.spark.sql.SparkSession

object OrcReader {
  def main(args: Array[String]): Unit = {

    val PREFIX_PATH="D:\\data\\flight-data\\orc"

    val spark=SparkSession.builder()
        .master("local")
        .appName("OrcReader")
        .getOrCreate()

    spark.read.format("orc")
      .load(PREFIX_PATH+"\\2010-summary.orc")
      .show(5)

  }
}
