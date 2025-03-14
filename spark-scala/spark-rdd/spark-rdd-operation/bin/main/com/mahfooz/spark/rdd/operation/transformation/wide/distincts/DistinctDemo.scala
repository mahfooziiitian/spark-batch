package com.mahfooz.spark.rdd.operation.transformation.wide.distincts

import org.apache.spark.sql.SparkSession

object DistinctDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val duplicateValueRDD = sc.parallelize(
      List("one", 1, "two", 2, "three", "one", "two", 1, 2)
    )

    duplicateValueRDD
      .distinct()
      .collect
      .foreach(println)

    println(duplicateValueRDD.distinct().count())
  }
}
