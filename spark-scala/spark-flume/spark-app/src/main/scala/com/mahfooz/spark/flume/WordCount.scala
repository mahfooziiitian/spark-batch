package com.mahfooz.spark.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.start()
    ssc.awaitTermination()
  }
}
