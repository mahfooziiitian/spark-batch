/*
RDD Checkpointing is a process of truncating RDD lineage graph and saving it to a reliable distributed (HDFS) or local file system.

Checkpointing is the act of saving an RDD to disk so that future references to this RDD point to those
intermediate partitions on disk rather than recomputing the RDD from its original source.

 */
package com.mahfooz.spark.rdd.checkpoint

import org.apache.spark.sql.SparkSession

object Checkpoints {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Checkpoints")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/text-book.txt"
    val data = sc.textFile(dataPath)
    val words = data.flatMap(lines => lines.split(" "))

    val checkpointDirectory = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/Processing/Spark/Checkpoint"
    spark.sparkContext.setCheckpointDir(s"${checkpointDirectory}/rdd")
    words.checkpoint()
  }
}
