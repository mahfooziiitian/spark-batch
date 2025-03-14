package com.mahfooz.spark.rdd.operation.transformation.narrow.map.partition

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

import scala.util.Random

object ForEachPartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ForEachPartition")
      .master("local")
      .getOrCreate()

    val fileRDD = spark.sparkContext.wholeTextFiles("D:\\data\\processing\\batch\\spark\\text\\spark_test.txt")

    val words = fileRDD.flatMap(line => line._2.split(" "))

    words.foreachPartition { iterator =>
      val randomFileName = new Random().nextInt()
      val pw =
        new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
      while (iterator.hasNext) {
        pw.write(iterator.next())
      }
      pw.close()
    }
  }

}
