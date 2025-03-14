/*

repartition() is used to increase or decrease the RDD, DataFrame, Dataset partitions.
Just increasing 1 partition results data movements from all partitions.
And, even decreasing the partitions also results in moving data from all partitions.
Hence when you wanted to decrease the partition recommendation is to use coalesce().
 */
package com.mahfooz.dataframe.partition.repartition

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Repartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Repartition")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("d:/data/flight-data/json/2015-summary.json")

    println(df.rdd.getNumPartitions)

    val partitionedDf=df.repartition(5)

    println(partitionedDf.rdd.getNumPartitions)

    df.repartition(5, col("DEST_COUNTRY_NAME")).write
      .mode(SaveMode.Overwrite)
      .csv("D:\\data\\processing\\batch\\spark\\partition\\repartition.csv")
  }
}
