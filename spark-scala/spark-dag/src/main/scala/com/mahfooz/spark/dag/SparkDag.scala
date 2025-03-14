/*
At a high level, when an action is called on the RDD, Spark creates the DAG and submits
the DAG to the DAG scheduler.

  1. The DAG scheduler divides operators such as map, flatMap, and so on into stages of tasks.
  2. The result of a DAG scheduler is a set of stages.
  3. The stages are passed on to the Task Scheduler.
  4. The Task Scheduler launches tasks via Cluster Manager.
  5. The worker executes the tasks

The word count problem consists of two stages.

a)  stage1
    The operators that do not require shuffling (flatMap() and map() in this case) are grouped together as Stage 1

b)  stage2
    The operators that require shuffling (reduceByKey) are grouped together as Stage 2.

 */
package com.mahfooz.spark.dag

import org.apache.spark.sql.SparkSession

object SparkDag {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(SparkDag.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext

    val reduceData=sc.textFile("D:\\data\\spark\\text\\people.txt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()

  }
}
