/*

SequenceFileRDD is created from a SequenceFile which is a format of files in the Hadoop File System.
The SequenceFile can be compressed or uncompressed.

 */
package com.mahfooz.spark.rdd.types.file

import org.apache.spark.sql.SparkSession

object SequenceFileRDDEx {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SequenceFileRDDEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val statesPopulationRDD =
      sc.textFile("D:\\data\\spark\\csv\\statesPopulation.csv")

    val pairRDD = statesPopulationRDD.map(
      record => (record.split(",")(0), record.split(",")(2))
    )

    pairRDD.saveAsSequenceFile("D:\\data\\spark\\sequence\\seqfile")

    //Reading save sequence file
    val seqRDD = sc.sequenceFile[String, String]("D:\\data\\spark\\sequence\\seqfile")

    seqRDD.take(10).foreach(record=>println(record))

  }
}
