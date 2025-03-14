package com.mahfooz.dataframe.columns.stats

import org.apache.spark.sql.SparkSession

object DescribeColumn {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DescribeColumn")
      .config("spark.sql.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .getOrCreate()

    val movies=spark.read.json("D:\\data\\movies\\movies.json")
    movies.describe("produced_year").show
  }

}
