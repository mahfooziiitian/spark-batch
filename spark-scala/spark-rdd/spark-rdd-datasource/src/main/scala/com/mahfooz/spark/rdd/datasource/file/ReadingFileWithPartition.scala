package com.mahfooz.spark.rdd.datasource.file

import org.apache.spark.sql.SparkSession

object ReadingFileWithPartition {

  val dataPath = "D:\\data\\spark\\text\\people.txt"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(ReadingFileWithPartition.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.textFile(dataPath, 3)

    val partitionRdd = rdd.mapPartitionsWithIndex((index, iterator) => {
      iterator.map(x => (index, x))
    })

    partitionRdd.foreach(println)

    spark.stop()
  }
}
