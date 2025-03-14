package com.mahfooz.spark.rdd.operation.transformation.wide.sampling

import org.apache.spark.sql.SparkSession

object Sampling {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName(Sampling.getClass.getName)
      .getOrCreate()

    val numbers =
      spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

    numbers.sample(true, 0.3).collect.foreach(println)

  }
}
