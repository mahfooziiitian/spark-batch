package com.mahfooz.spark.rdd.operation.action.count

import org.apache.spark.sql.SparkSession

object CountApproxAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CountApproxAction")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"

    val data = sc.textFile(dataPath)

    val words = data.flatMap(lines => lines.split(" "))

    val confidence = 0.95
    val timeoutMilliseconds = 4000
    println(words.countApprox(timeoutMilliseconds, confidence))

    println(words.countApproxDistinct(0.05))

    println(words.countByValue())

    println(words.countByValueApprox(1000, 0.95))
  }

}
