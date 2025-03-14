package com.mahfooz.spark.rdd.operation.action.pair.lookup

import org.apache.spark.{SparkConf, SparkContext}

object Lookup {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(Lookup.getClass.getName)
      .setMaster("local")

    val sc = new SparkContext(conf)

    val candyTx = sc.parallelize(
      List(("candy1", 5.2), ("candy2", 3.5), ("candy1", 2.0), ("candy3", 6.0))
    )

    candyTx.lookup("candy1").foreach(println)

    candyTx.lookup("candy2").foreach(println)

    candyTx.lookup("candy5").foreach(println)

  }
  
}
