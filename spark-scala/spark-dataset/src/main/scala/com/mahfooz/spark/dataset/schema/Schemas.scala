/*

A schema defines the column names and types of a DataFrame.
You can define schemas manually or read a schema from a data source (often called schema on read).

val flightsDF =spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("2015-summary.csv")

=========KNOW THE SCHEMA=================================
flightsDF.printSchema()


 */
package com.mahfooz.spark.dataset.schema

import org.apache.spark.sql.SparkSession

case class ValueAndDouble(value:Long, valueDoubled:Long)

object Schemas {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val count=spark.range(2000)
      .map(value => ValueAndDouble(value, value * 2))
      .filter(vAndD => vAndD.valueDoubled % 2 == 0)
      .where("value % 3 = 0") .count()

    println(count)
  }
}
