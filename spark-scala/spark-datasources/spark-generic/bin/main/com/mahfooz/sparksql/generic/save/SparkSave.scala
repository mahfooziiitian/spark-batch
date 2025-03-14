/*

The default data source (parquet unless otherwise configured by spark.sql.sources.default) will be used for all
operations.

 */
package com.mahfooz.sparksql.generic.save

import org.apache.spark.sql.SparkSession

object SparkSave {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkSave")
      .master("local[*]")
      .getOrCreate()

    val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
    usersDF
      .select("name", "favorite_color")
      .write
      .save("namesAndFavColors.parquet")
  }
}
