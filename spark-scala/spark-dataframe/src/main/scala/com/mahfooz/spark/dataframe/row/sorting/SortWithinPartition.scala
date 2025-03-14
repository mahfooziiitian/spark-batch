package com.mahfooz.spark.dataframe.row.sorting

import org.apache.spark.sql.SparkSession

object SortWithinPartition {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SortWithinPartition")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("json")
      .load(getClass.getResource("/2015-summary.json").getFile)
      .sortWithinPartitions("count")

    df.show(20)

  }
}
