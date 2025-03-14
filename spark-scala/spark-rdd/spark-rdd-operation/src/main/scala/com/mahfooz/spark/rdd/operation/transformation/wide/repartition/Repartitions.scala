/*

Stages in Spark represent groups of tasks that can be executed together to compute the same operation on
multiple machines.
In general, Spark will try to pack as much work as possible (i.e., as many transformations as possible inside
your job) into the same stage, but the engine starts new stages after operations called shuffles.
A shuffle represents a physical repartitioning of the data—for example, sorting a DataFrame, or grouping data
that was loaded from a file by key (which requires sending records with the same key to the same node).
This type of repartitioning requires coordinating across executors to move data around.
Spark starts a new stage after each shuffle, and keeps track of what order the stages must run in to compute
the final result.
By default when you create a DataFrame with range, it has eight partitions. The next step is the repartitioning.
This changes the number of partitions by shuffling the data.

spark.sql.shuffle.partitions
spark.conf.set("spark.sql.shuffle.partitions", 50)

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.repartition

import org.apache.spark.sql.SparkSession

object Repartitions {

  def main(args: Array[String]): Unit = {

    val prefixPath="D:\\data\\spark\\spark-by-examples"

    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName(Repartitions.getClass.getName)
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Range(0,20))
    println("From local[5] : "+rdd.partitions.size)

    val rdd1 = spark.sparkContext.parallelize(Range(0,20), 6)
    println("parallelize : "+rdd1.partitions.size)

    rdd1.partitions.foreach(f=> f.toString)

    rdd1.saveAsTextFile("d:/tmp/partition")

    val rdd2 = rdd1.repartition(4)
    println("Repartition size : "+rdd2.partitions.size)

    rdd2.saveAsTextFile("d:/tmp/re-partition")

    val rdd3 = rdd1.coalesce(4)
    println("Repartition size : "+rdd3.partitions.size)

    rdd3.saveAsTextFile("d:/tmp/coalesce")

    val rddFromFile = spark.sparkContext.textFile(prefixPath+"\\test.txt",9)

    println("TextFile : "+rddFromFile.partitions.size)

  }
}
