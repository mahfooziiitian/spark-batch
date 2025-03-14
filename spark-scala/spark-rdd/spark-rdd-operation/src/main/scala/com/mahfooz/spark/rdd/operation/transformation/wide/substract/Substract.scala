package com.mahfooz.spark.rdd.operation.transformation.wide.substract

import org.apache.spark.sql.SparkSession

object Substract {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master ("local")
      .appName(Substract.getClass.getName)
      .getOrCreate ()

    val words = spark.sparkContext.parallelize(
      List("The amazing thing about spark is that it is very simple to learn"))
      .flatMap(l => l.split(" ")).
      map(w => w.toLowerCase)

    val stopWords = spark.sparkContext.parallelize(List("the it is to that")).
      flatMap(l => l.split(" "))

    val realWords = words.subtract(stopWords)
    realWords.collect().foreach(println)
  }

}
