/*

spark.sql.warehouse.dir

 */
package com.mahfooz.spark.config.session

import org.apache.spark.sql.SparkSession

object SessionConfig {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .config("spark.master","local[*]")
      .appName("SessionConfig")
      .getOrCreate()

    val movies=spark.read
      .format("json")
      .load(args(0))

    movies.show(6)

    spark.stop()
  }
}
