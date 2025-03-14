/*

The SparkConf manages all of our application configurations.


1)  Get the configuration from spark-shell
    spark.conf.getAll.foreach(println)
2)

 */
package com.mahfooz.spark.config

import org.apache.spark.{SparkConf, SparkContext}

object SparkConfigs {

  def main(args: Array[String]): Unit = {

    println("Creating Spark Configuration")
    val conf = new SparkConf()
    conf.setAppName("spark-conf")
    conf.setMaster("local")

    println("Creating Spark Context")
    val sc=new SparkContext(conf)

    println("Loading the Dataset and will further process it")
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    distData.foreach(println)
  }
}
