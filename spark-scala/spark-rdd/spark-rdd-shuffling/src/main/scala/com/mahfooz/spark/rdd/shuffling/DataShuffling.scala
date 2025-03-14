/*

Certain key/value transformations and actions require moving data from multiple partitions to other partitions, meaning
across executors and machines.
This process is known as the shuffle, which is quite important to be familiar with because it is an expensive operation.
During the shuffling process, the data needs to be serialized, written to disk, transferred over the network, and
finally deserialized.
Shuffling data will add latency to the completion of the data processing in your Spark jobs.
A subset of the transformations that fall into this category: groupByKey, reduceByKey, aggregateByKey, and join.
 Operations like aggregations (seen in the preceding section) require data to be moved across the cluster in a phase known as shuffling.

 */
package com.mahfooz.spark.rdd.shuffling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataShuffling {

  def main(args: Array[String]): Unit = {

    val prefixPath = "D:\\data\\spark\\spark-by-examples"

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(DataShuffling.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(prefixPath + "\\test.txt")

    println(rdd.getNumPartitions)

    val rdd2 = rdd
      .flatMap(record => record.split(" "))
      .map(key => (key, 1))

    //ReduceBy transformation
    val rdd5 = rdd2.reduceByKey(_ + _)

    println(rdd5.getNumPartitions)
  }
}
