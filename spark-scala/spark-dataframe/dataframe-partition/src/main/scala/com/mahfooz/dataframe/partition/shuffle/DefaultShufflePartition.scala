package com.mahfooz.dataframe.partition.shuffle

import org.apache.spark.sql.SparkSession

object DefaultShufflePartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[5]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load("D:\\data\\flight-data\\json\\2015-summary.json")

    val countDf = df.groupBy("DEST_COUNTRY_NAME").count()
    println(countDf.rdd.getNumPartitions)

  }

}
