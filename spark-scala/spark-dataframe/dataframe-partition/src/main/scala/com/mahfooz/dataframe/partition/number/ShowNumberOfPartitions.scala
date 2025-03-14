package com.mahfooz.dataframe.partition.number

import org.apache.spark.sql.{SaveMode, SparkSession}

object ShowNumberOfPartitions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(ShowNumberOfPartitions.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val movies = spark.read
      .format("json")
      .load("D:\\data\\flight-data\\json\\2015-summary.json")

    println(movies.rdd.getNumPartitions)

    movies.write
      .mode(SaveMode.Overwrite)
      .csv("D:\\data\\processing\\batch\\spark\\partition\\partition.csv")

  }
}
