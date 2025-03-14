package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

object CountByKeys {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf().setAppName("CountByKeys").setMaster("local")
    val sc = new SparkContext(conf)
    val candyTx = sc.parallelize(
      List(("candy1", 5.2), ("candy2", 3.5), ("candy1", 2.0), ("candy3", 6.0))
    )
    candyTx.countByKey.foreach(println)
  }
}
