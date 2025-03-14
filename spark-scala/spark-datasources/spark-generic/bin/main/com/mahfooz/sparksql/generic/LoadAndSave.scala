/*

The default data source is parquet.

spark.sql.sources.default

 */
package com.mahfooz.sparksql.generic

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadAndSave {

  def main(args: Array[String]): Unit = {

    val inputFile=args(0)
    val outputFile=args(1)
    val spark = SparkSession.builder
      .appName("LoadAndSave")
      .master("local[*]")
      .getOrCreate()

    val usersDF = spark
      .read
      .load(inputFile)

    usersDF.printSchema()

    usersDF
      .select("name", "favorite_color")
      .write
      .mode(SaveMode.Overwrite)
      .save(outputFile)
  }
}
