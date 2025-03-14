package com.mahfooz.spark.dataframe.row.splits

import org.apache.spark.sql.SparkSession

object RowSplts {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load(args(0))

    val seed = 5
    val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
    dataFrames.foreach(record => println(record.count()))
    println(dataFrames(0).count() > dataFrames(1).count())

  }
}
