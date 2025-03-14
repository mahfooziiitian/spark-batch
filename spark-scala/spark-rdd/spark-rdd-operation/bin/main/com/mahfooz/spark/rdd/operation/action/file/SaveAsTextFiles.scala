package com.mahfooz.spark.rdd.operation.action.file

import org.apache.spark.sql.SparkSession

object SaveAsTextFiles {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SaveAsTextFiles.getClass.getName)
      .master("local")
      .getOrCreate()

    val data = spark.read
      .textFile(
        getClass
          .getResource("/spark_test.txt")
          .getFile
      ).rdd

    val words=data.flatMap(line => line.split(" "));
    words.saveAsTextFile("file:/d:/tmp/bookTitle")
  }
}
