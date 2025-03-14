package com.mahfooz.spark.rdd.operation.transformation.wide.key

import org.apache.spark.sql.SparkSession

object SparkKeyBy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkKeyBy")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"
    val data = sc.textFile(dataPath)
    val words = data.flatMap(lines => lines.split("\\s+"))
    val keyword = words.keyBy(word => word.toLowerCase)

    keyword.foreach(println)
  }

}
