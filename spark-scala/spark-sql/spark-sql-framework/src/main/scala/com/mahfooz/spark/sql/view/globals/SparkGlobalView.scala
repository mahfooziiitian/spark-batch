package com.mahfooz.spark.sql.view.globals

import org.apache.spark.sql.SparkSession

object SparkGlobalView {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("SparkGlobalView")
      .master("local[*]")
      .getOrCreate()

    val movies=spark.read.json("D:\\data\\movies\\movies.json")

    movies.createOrReplaceGlobalTempView("movies_g")

    spark.sql("select count(*) from global_temp.movies_g").show
  }

}
