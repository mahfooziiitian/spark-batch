package com.mahfooz.sparksql.generic.expr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object GenericExpression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val flightData2015 = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(args(0))

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .explain
  }
}
