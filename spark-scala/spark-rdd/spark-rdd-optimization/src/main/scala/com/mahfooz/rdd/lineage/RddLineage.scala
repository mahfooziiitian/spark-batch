package com.mahfooz.rdd.lineage

import org.apache.spark.sql.SparkSession

object RddLineage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("RddLineage")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"

    val wordCount = sc
      .textFile(dataPath)
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    println(wordCount.toDebugString)

    //The numbers in round brackets show the level of parallelism at each stage
    // e.g. (2) in the above output.
    println(wordCount.getNumPartitions)
  }

}
