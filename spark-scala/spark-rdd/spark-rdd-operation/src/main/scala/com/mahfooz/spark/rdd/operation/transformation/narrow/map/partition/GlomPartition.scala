package com.mahfooz.spark.rdd.operation.transformation.narrow.map.partition

import org.apache.spark.sql.SparkSession

object GlomPartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    sc.parallelize(Seq("Hello", "World"), 2)
      .glom()
      .collect()
      .foreach(println)
  }
}