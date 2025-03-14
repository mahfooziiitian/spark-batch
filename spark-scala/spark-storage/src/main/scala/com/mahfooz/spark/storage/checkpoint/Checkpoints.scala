package com.mahfooz.spark.storage.checkpoint

import org.apache.spark.sql.SparkSession

object Checkpoints {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.sparkContext

    // Set the checkpoint directory (should be a reliable distributed file system like HDFS)
    val checkPointDir = s"${sys.env("DATA_HOME").replace("\\","/")}/Processing/Batch/Spark/checkpoint/lineage_break"

    sc.setCheckpointDir(checkPointDir)

    // Sample data
    val data = List(1, 2, 3, 4, 5)

    // Create an RDD
    val rdd = sc.parallelize(data)

    // Transformation 1
    val squaredRDD = rdd.map(x => x * x)

    // Transformation 2
    val filteredRDD = squaredRDD.filter(x => x > 5)

    // Checkpoint the RDD
    filteredRDD.checkpoint()

    // Action
    val result = filteredRDD.collect()

    result.foreach(println)

    Thread.sleep(200000)

    // Stop the Spark context
    sc.stop()

  }
}
