package com.mahfooz.spark.rdd.operation.transformation.narrow.map

import org.apache.spark.sql.SparkSession

object MapDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val data = sc.textFile(getClass
      .getResource("/spark_test.txt").getFile)

    val mapFile = data.map(line => (line, line.length))
    mapFile.foreach(println)

    val allCapsRDD = data.map(line => line.toUpperCase)
    allCapsRDD.collect().foreach(println)

    val words=data.map(word => word.split(" "))

    val words2=words.map(word => (word, word(0), word.startsWith("S")))

    words2.foreach(println)

    words2.filter(record => record._3).take(5).foreach(println)

  }
}
