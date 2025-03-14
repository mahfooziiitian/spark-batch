package com.mahfooz.spark.rdd.operation.transformation.wide.intersection

import org.apache.spark.sql.SparkSession

object Intersection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master ("local")
      .appName(Intersection.getClass.getName)
      .getOrCreate ()

    val rdd1 = spark.sparkContext.parallelize(Array("One", "Two", "Three"))
    val rdd2 = spark.sparkContext.parallelize(Array("two","One","threed","One"))
    val rdd3 = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)
  }
}
