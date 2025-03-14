package com.mahfooz.dataframe.columns.rename

import org.apache.spark.sql.SparkSession

object SparkColumnRename {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SparkColumnRename.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    movies.withColumnRenamed("actor_name", "actor")
      .withColumnRenamed("movie_title", "title")
      .withColumnRenamed("produced_year", "year").show(5)
  }
}
