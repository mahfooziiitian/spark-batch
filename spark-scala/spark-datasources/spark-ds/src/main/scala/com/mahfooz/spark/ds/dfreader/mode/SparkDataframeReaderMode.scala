/*

Reading data from an external source naturally entails encountering malformed data, especially
when working with only semi-structured data sources.

Read modes specify what will happen when Spark does come across malformed records.

Read mode

permissive

  Sets all fields to null when it encounters a corrupted record and places all corrupted records in a string column called _corrupt_record

dropMalformed

  Drops the row that contains malformed records

failFast
  Fails immediately upon encountering malformed records

The default is permissive.
 */
package com.mahfooz.spark.ds.dfreader.mode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkDataframeReaderMode {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkDataframeReaderMode")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    val schema = StructType(
      Array(
        StructField("actor",StringType,false),
        StructField("title",StringType,false),
        StructField("year",IntegerType,false)
      )
    )

    val moviesDf = spark.read.format("csv")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .option("path", s"${dataHome}/FileData/Csv/Movies/movies.csv")
      .schema(schema)
      .load()

    moviesDf.printSchema()
  }

}
