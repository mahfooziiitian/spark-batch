/*

RangePartitioner works by partitioning the RDD into roughly equal ranges.
Since the range has to know the starting and ending keys for any partition, the RDD needs to be sorted first
before a RangePartitioner can be used.
RangePartitioning first needs reasonable boundaries for the partitions based on the RDD and then create a
function from key K to the partitionIndex where the element belongs.
Finally, we need to repartition the RDD, based on the RangePartitioner to distribute the RDD elements
correctly as per the ranges we determined.
 */
package com.mahfooz.spark.rdd.partition.partitioners.range

import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

object RangePartitionerEx {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(RangePartitionerEx.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext
    val statesPopulationRDD =
      sc.textFile("D:\\data\\spark\\csv\\statesPopulation.csv")
    val pairRDD = statesPopulationRDD.map(record => (record.split(",")(0), 1))
    val rangePartitioner = new RangePartitioner(5, pairRDD)
    val rangePartitionedRDD = pairRDD.partitionBy(rangePartitioner)
    pairRDD
      .mapPartitionsWithIndex((i, x) => Iterator("" + i + ":" + x.length))
      .take(10)
      .foreach(record => println(record))

    rangePartitionedRDD
      .mapPartitionsWithIndex((i, x) => Iterator("" + i + ":" + x.length))
      .take(10)
      .foreach(record => println(record))
  }
}
