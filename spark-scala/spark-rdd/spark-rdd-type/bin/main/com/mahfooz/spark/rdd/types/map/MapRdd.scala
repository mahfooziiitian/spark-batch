package com.mahfooz.spark.rdd.types.map

import org.apache.spark.sql.SparkSession

object MapRdd {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("MapRdd")
      .getOrCreate()

    val sc =spark.sparkContext

    val rdd_one = sc.parallelize(Seq(1,2,3,4,5,6))

    rdd_one.take(100).foreach(element=>println(element))

    val rdd_two = rdd_one.map(i => i * 3)
    println(rdd_two)

    println(rdd_one.toDebugString)
    println(rdd_two.toDebugString)

  }

}
