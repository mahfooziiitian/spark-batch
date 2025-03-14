package com.mahfooz.spark.rdd.pipe

import org.apache.spark.sql.SparkSession

object RddPipe {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("RddPipe")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"
    val data = sc.textFile(dataPath)
    val words = data.flatMap(lines => lines.split(" "))

    words.pipe("find /i \"am\"").collect()
      .foreach(println)
  }
}
