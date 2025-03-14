package com.mahfooz.dataframe.columns.remove

import org.apache.spark.sql.SparkSession

object RemovingColumn {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val movies = spark.read.json("D:\\data\\movies\\movies.json")

    movies.drop("movie_title")
      .show(2)

  }
}
