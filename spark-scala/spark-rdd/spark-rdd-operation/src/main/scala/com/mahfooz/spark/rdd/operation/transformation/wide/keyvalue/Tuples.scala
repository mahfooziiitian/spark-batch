package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object Tuples {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName(Tuples.getClass.getName)
      .getOrCreate()

    val sc=spark.sparkContext

    val rdd = sc.parallelize(List("Spark","is","an", "amazing", "piece", "is","of","technology"))

    val pairRDD = rdd.map(w => (w.length,w))

    pairRDD.collect().foreach(println)
  }
}
