package com.mahfooz.spark.rdd.operation.transformation.filter

import org.apache.spark.sql.SparkSession

object SQLLikeFilter {

  def startsWithS(individual:String) = {
    individual.startsWith("S")
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName ("SQLLikeFilter")
      .master ("local")
      .getOrCreate ()

    val sc=spark.sparkContext

    val data = sc.textFile (getClass
      .getResource("/spark_test.txt").getFile)

    val words=data.flatMap(lines => lines.split(" "))

    words.filter(word => startsWithS(word))
      .collect()
      .foreach(println)
  }
}
