package com.mahfooz.spark.rdd.operation.action.top

import org.apache.spark.sql.SparkSession

object TopAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName(TopAction.getClass.getName)
      .getOrCreate()

    val numberRDD = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,11,8,9,10), 2)
    numberRDD.top(4).foreach(println)
  }
}
