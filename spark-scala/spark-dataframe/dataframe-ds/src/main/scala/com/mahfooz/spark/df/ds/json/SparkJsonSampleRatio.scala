package com.mahfooz.spark.df.ds.json

import org.apache.spark.sql.SparkSession

object SparkJsonSampleRatio {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkJsonSampleRatio")
      .master("local[*]")
      .getOrCreate()

    //read multiline json file
    val multiline_df = spark.read
      .option("multiline", "true")
      .option("samplingRatio", "1.0")
      .json("D:\\data\\processing\\batch\\spark\\json\\multiline-zipcode.json")
    multiline_df.printSchema()
    multiline_df.show(false)

  }
}
