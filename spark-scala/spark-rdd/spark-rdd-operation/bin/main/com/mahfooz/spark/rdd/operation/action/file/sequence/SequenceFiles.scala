package com.mahfooz.spark.rdd.operation.action.file.sequence

import org.apache.spark.sql.SparkSession

object SequenceFiles {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SequenceFiles")
      .master("local")
      .getOrCreate()

    val data = spark.read
      .textFile(
        getClass
          .getResource("/spark_test.txt")
          .getFile
      ).rdd

    val words=data.flatMap(line => line.split(" "))
    words.saveAsObjectFile("file:/C:/tmp/my/sequenceFilePath")
  }
}
