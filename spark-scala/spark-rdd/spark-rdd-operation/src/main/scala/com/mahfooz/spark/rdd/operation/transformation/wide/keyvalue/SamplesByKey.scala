package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession
import scala.util.Random

object SamplesByKey {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val rdd=spark.sparkContext.wholeTextFiles(getClass
      .getResource("/spark_test.txt").getFile)

    val words=rdd.flatMap(line => line._2.split(" "))

    val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
      .collect()

    val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap

    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKey(true, sampleMap, 6L)
      .collect()
      .foreach(println)

    words.map(word => (word.toLowerCase.toSeq(0), word))
      .sampleByKeyExact(true, sampleMap, 6L).collect()
      .foreach(println)

  }
}
