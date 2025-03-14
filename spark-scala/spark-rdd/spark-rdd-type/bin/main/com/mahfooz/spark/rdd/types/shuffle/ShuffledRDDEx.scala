/*

ShuffledRDD shuffles the RDD elements by key so as to accumulate values for the same key on the
same executor to allow an aggregation or combining logic.

class ShuffledRDD[K, V, C] extends RDD[(K, C)]

 */
package com.mahfooz.spark.rdd.types.shuffle

import org.apache.spark.sql.SparkSession

object ShuffledRDDEx {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ShuffledRDDEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val statesPopulationRDD =
      sc.textFile("D:\\data\\spark\\csv\\statesPopulation.csv")
    val pairRDD = statesPopulationRDD.map(record => (record.split(",")(0), 1))

    val shuffledRDD = pairRDD.reduceByKey(_+_)
    print(shuffledRDD)

    shuffledRDD.take(5).foreach(record=>println(record))
  }
}
