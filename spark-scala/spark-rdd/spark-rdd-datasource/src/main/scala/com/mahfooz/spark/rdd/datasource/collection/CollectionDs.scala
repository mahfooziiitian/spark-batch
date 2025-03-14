package com.mahfooz.spark.rdd.datasource.collection

import org.apache.spark.sql.SparkSession

object CollectionDs {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .master("local")
      .appName("CollectionDs")
      .getOrCreate()

    val sc=spark.sparkContext

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")

    //This will return rdd
    val words = sc.parallelize(myCollection, 2)
    words.foreach(println)
    words.setName("myWords")

    println("Number of partitions = "+words.getNumPartitions)
    println("Id of RDD  = "+words.id)
    println("Name of RDD = "+words.name)
  }
}
