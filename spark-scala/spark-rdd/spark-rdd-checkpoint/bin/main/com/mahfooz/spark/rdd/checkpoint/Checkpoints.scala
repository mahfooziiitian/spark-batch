/*
RDD Checkpointing is a process of truncating RDD lineage graph and saving it to a reliable distributed (HDFS) or local file system.

Checkpointing is the act of saving an RDD to disk so that future references to this RDD point to those
intermediate partitions on disk rather than recomputing the RDD from its original source.

 */
package com.mahfooz.spark.rdd.checkpoint

import org.apache.spark.sql.SparkSession

object Checkpoints {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Checkpoints")
      .master("local")
      .getOrCreate()

    val fileRDD = spark.sparkContext.wholeTextFiles(
      getClass
        .getResource("/spark_test.txt")
        .getFile
    )

    val words = fileRDD.flatMap(line => line._2.split(" "))
    spark.sparkContext.setCheckpointDir("file:/C:/tmp/checkpointing")
    words.checkpoint()
  }
}
