package com.mahfooz.spark.rdd.operation.action.maxmin

import org.apache.spark.sql.SparkSession

object MaxMin {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    println(spark.sparkContext.parallelize(1 to 20).max())
    println(spark.sparkContext.parallelize(1 to 20).min())


  }
}
