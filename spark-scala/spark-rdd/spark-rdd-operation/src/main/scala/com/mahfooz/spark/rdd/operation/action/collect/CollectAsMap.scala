package com.mahfooz.spark.rdd.operation.action.collect

import org.apache.spark.{SparkConf, SparkContext}

object CollectAsMap {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(CollectAsMap.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val candyTx = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
      ("candy1", 2.0), ("candy3", 6.0)))

    candyTx.collectAsMap().foreach(println)
  }
}
