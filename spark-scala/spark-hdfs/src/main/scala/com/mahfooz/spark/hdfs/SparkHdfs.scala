package com.mahfooz.spark.hdfs

import org.apache.spark.{SparkConf, SparkContext}

object SparkHdfs {

  def main(args: Array[String]): Unit = {
    val logFile = "hdfs://mtthdopms02-10g.intl.vsnl.co.in:8020/user/hdfs/data.txt"
    val sparkConf = new SparkConf().setAppName("Spark Word Count")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(logFile)
    val counts = file.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://mtthdopms02-10g.intl.vsnl.co.in:8020/user/hdfs/output")
  }
}
