/*

Since we are reducing 5 to 2 partitions, the data movement happens only from 3 partitions and it moves to remain
2 partitions.
Spark DataFrame coalesce() is used only to decrease the number of partitions.
This is an optimized or improved version of repartition() where the movement of the data across the partitions is
fewer using coalesce.

 */
package com.mahfooz.dataframe.partition.coalesce

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Coalesces {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(Coalesces.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load("D:\\data\\flight-data\\json\\2015-summary.json")

    df.repartition(5, col("DEST_COUNTRY_NAME"))
      .coalesce(2)
      .write
      .mode(SaveMode.Overwrite)
      .csv("d:/data/processing/batch/spark/partition/coalesce.csv")

    val df3 = df.repartition(5).coalesce(2)
    println(df3.rdd.partitions.length)
  }
}
