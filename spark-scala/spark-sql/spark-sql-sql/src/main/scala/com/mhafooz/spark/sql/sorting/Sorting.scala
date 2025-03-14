/*

 */
package com.mhafooz.spark.sql.sorting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Sorting {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.getOrElse("DATA_HOME","")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Sorting")
      .getOrCreate()

    val statesDF = spark.read.option("header", "true")
      .option("inferschema", "true")
      .option("sep", ",")
      .csv(s"${dataHome}/statesPopulation.csv")

    statesDF.sort(col("Population").desc).show(5)
  }
}
