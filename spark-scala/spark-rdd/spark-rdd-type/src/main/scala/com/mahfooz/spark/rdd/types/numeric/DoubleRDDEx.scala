package com.mahfooz.spark.rdd.types.numeric

import org.apache.spark.sql.SparkSession

object DoubleRDDEx {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DoubleRDDEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd_one = sc.parallelize(Seq(1.0,2.0,3.0))

    println(rdd_one.mean)
  }

}
