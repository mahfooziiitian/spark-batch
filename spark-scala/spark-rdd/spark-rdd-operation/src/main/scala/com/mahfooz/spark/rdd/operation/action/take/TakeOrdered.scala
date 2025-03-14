package com.mahfooz.spark.rdd.operation.action.take

import org.apache.spark.sql.SparkSession

object TakeOrdered {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName(TakeOrdered.getClass.getName)
      .getOrCreate()

    val numberRDD = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9,10), 2)

    numberRDD.takeOrdered(4).foreach(println)

    numberRDD.takeOrdered(4)(Ordering[Int].reverse).foreach(println)

  }
}
