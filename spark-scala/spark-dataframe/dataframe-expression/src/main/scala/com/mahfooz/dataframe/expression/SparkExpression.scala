/*
This transformation is a variant of the select transformation.
The one big difference is that it accepts one or more SQL expressions, rather than columns
You can select, manipulate, and remove columns from DataFrames and these operations are represented as expressions.
To Spark, columns are logical constructions that simply represent a value computed on a per-record basis by means of an
expression. This means that to have a real value for a column, we need to have a row; and to have a row, we need to have a
DataFrame. You cannot manipulate an individual column outside the context of a DataFrame;
you must use Spark transformations within a DataFrame to modify the contents of a column.

An expression is a set of transformations on one or more values in a record in a DataFrame

 */
package com.mahfooz.dataframe.expression

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object SparkExpression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load("D:\\data\\flight-data\\json\\2015-summary.json")

    df.select(
        df.col("DEST_COUNTRY_NAME"),
        col("DEST_COUNTRY_NAME"),
        column("DEST_COUNTRY_NAME"),
        expr("DEST_COUNTRY_NAME"),
        expr("DEST_COUNTRY_NAME AS destination")
      )
      .show(2)
  }
}
