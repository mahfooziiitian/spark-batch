package com.mahfooz.spark.rdd.operation.action.reduce

import org.apache.spark.sql.SparkSession

object CustomReduceAction {

  def wordLengthReducer(leftWord: String, rightWord: String): String = {
    if (leftWord.length > rightWord.length)
      return leftWord
    else
      return rightWord
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CustomReduceAction")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val data = sc.textFile(
      getClass
        .getResource("/spark_test.txt")
        .getFile
    )

    val words = data.flatMap(lines => lines.split(" "))
    println(words.reduce(wordLengthReducer))

  }

}
