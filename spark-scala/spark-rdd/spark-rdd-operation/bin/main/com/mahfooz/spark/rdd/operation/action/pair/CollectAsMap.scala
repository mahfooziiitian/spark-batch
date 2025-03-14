package com.mahfooz.spark.rdd.operation.action.pair

import org.apache.spark.{SparkConf, SparkContext}

object CollectAsMap {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(CollectAsMap.getClass.getName)
      .setMaster("local")

    val sc = new SparkContext(conf)

    val candyTx = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
      ("candy1", 2.0), ("candy3", 6.0)))

    //if the dataset contains multiple rows with the same key, it will be collapsed
    //into a single entry in the map. There are four rows in the candyTx pair RDD; however,
    //there are only three rows in the output. Two candy1 rows are collapsed into a single row.
    candyTx.collectAsMap().foreach(println)
  }
}
