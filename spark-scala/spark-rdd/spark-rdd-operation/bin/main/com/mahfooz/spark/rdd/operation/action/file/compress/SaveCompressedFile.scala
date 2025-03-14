package com.mahfooz.spark.rdd.operation.action.file.compress

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.SparkSession

object SaveCompressedFile {
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
    words.saveAsTextFile("file:/tmp/bookTitleCompressed", classOf[BZip2Codec])
  }
}
