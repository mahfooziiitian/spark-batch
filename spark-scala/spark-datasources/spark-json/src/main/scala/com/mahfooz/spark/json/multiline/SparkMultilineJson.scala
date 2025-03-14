/*
The line-delimited versus multiline trade-off is controlled by a single option: multiLine.

When you set this option to true, you can read an entire file as one json object and Spark will go
through the work of parsing that into a DataFrame.

 */
package com.mahfooz.spark.json.multiline

import org.apache.spark.sql.SparkSession

object SparkMultilineJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movies =
      spark.read
        .option("multiLine", true)
        .json(args(0))

    movies.show(10)
  }
}
