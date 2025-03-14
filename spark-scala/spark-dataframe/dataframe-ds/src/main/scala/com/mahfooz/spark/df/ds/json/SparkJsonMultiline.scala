package com.mahfooz.spark.df.ds.json

import org.apache.spark.sql.SparkSession

object SparkJsonMultiline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkJsonMultiline")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .option("multiLine", "true")
      .load("D:\\data\\processing\\batch\\spark\\json\\multiline.json")

    df.printSchema

    df.show(truncate = false)

  }
}
