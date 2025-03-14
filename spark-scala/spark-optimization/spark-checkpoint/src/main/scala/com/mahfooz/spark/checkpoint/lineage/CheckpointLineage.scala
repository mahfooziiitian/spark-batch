package com.mahfooz.spark.checkpoint.lineage

import org.apache.spark.sql.SparkSession

object CheckpointLineage {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("checkpoint-lineage")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val checkpoint_directory = sys.env
      .getOrElse("DATA_HOME", "data") + "\\spark\\checkpoint"
    sc.setCheckpointDir(checkpoint_directory)

    val rdd = sc.parallelize(1 to 10)
      .map(x => (x % 3, 1))
      .reduceByKey(_ + _)

    val indChk = rdd.mapValues(_ > 4)
    indChk.checkpoint
    println(indChk.toDebugString)

    println(indChk.count)
    println(indChk.toDebugString)

  }

}
