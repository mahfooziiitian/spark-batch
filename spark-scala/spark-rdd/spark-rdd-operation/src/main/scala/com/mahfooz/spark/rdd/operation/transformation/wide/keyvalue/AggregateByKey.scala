/*

aggregateByKey is quite similar to reduceByKey, except that aggregateByKey allows more flexibility and customization of how to aggregate within partitions and between partitions to allow much more sophisticated use cases such as generating a list of all <Year, Population> pairs as well as total population for each State in one function call.

def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
 combOp: (U, U) => U): RDD[(K, U)]

def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
 combOp: (U, U) => U): RDD[(K, U)]

def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
 combOp: (U, U) => U): RDD[(K, U)]

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object AggregateByKey {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("AggregateByKey")
      .getOrCreate()

    val sc = spark.sparkContext

    val statesPopulationRDD = sc
      .textFile("D:\\data\\processing\\spark\\csv\\statesPopulation.csv")
      .filter(_.split(",")(0) != "State")

    val pairRDD = statesPopulationRDD
      .map(record => record.split(","))
      .map(t => (t(0), (t(1).toInt, t(2).toInt)))

    val initialSet = 0

    val addToSet = (s: Int, v: (Int, Int)) => s + v._2
    val mergePartitionSets = (p1: Int, p2: Int) => p1 + p2

    val aggregatedRDD =
      pairRDD.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    aggregatedRDD.take(10).foreach(record => println(record))
  }
}
