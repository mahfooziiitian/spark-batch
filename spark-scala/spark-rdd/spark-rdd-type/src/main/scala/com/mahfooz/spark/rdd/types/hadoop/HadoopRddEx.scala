/*

class HadoopRDD[K, V] extends RDD[(K, V)]

 */
package com.mahfooz.spark.rdd.types.hadoop

import org.apache.spark.sql.SparkSession

object HadoopRddEx {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HadoopRddEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val statesPopulationRDD =
      sc.textFile("D:\\data\\spark\\csv\\statesPopulation.csv")

    print(statesPopulationRDD.toDebugString)

  }
}
