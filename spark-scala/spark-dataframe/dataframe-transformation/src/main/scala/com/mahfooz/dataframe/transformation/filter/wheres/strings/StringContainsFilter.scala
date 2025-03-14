package com.mahfooz.dataframe.transformation.filter.wheres.strings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object StringContainsFilter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("StringContainsFilter")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    /*
    SELECT Description FROM dfTable
    WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
     */
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("DESCRIPTION").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where("hasSimpleColor")
      .select("Description").show(3, false)
  }

}
