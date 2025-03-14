package com.mahfooz.spark.rdd.operation

import org.apache.spark.sql.SparkSession

object RandomRddSplit {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SQLLikeFilter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"

    val words = sc.textFile(dataPath).flatMap(lines => lines.split(" "))
    val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

    fiftyFiftySplit.foreach(partition=> println(partition.getNumPartitions))

  }

}
