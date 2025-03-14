/*

A schema in Spark defines the column names and its associated data types for a DataFrame.
A schema defines the column names and types of a DataFrame.
You can define schemas manually or read a schema from a data source (often called schema on read).
In Scala, we can also take advantage of Spark’s implicits in the console by running toDF on a Seq type.


 */
package com.mahfooz.dataframe.schema

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

object Schemas {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Schemas")
      .master("local")
      .getOrCreate()

    //automatic schema using toDF
    val df = spark
      .range(500)
      .toDF("number")

    df.select(df.col("number") + 10)
    df.printSchema()

  }
}
