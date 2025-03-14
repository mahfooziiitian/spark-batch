package com.mahfooz.spark.rdd.operation.action.count

import org.apache.spark.sql.SparkSession

object CountAction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CountAction")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"

    val data = sc.textFile(dataPath)

    val words = data.flatMap(lines => lines.split(" "))

    println(words.count)
  }

}
