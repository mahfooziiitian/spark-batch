/*

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object CombineByKey {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CombineByKey")
      .getOrCreate()

    val sc = spark.sparkContext

    val statesPopulationRDD = sc
      .textFile("D:\\data\\processing\\spark\\csv\\statesPopulation.csv")
      .filter(_.split(",")(0) != "State")

    val pairRDD = statesPopulationRDD
      .map(record => record.split(","))
      .map(t => (t(0), (t(1).toInt, t(2).toInt)))

    val createCombiner = (x: (Int, Int)) => x._2

    val mergeValues = (c: Int, x: (Int, Int)) => c + x._2

    val mergeCombiners = (c1: Int, c2: Int) => c1 + c2

    val combinedRDD =
      pairRDD.combineByKey(createCombiner, mergeValues, mergeCombiners)

    combinedRDD.take(10).foreach(record => println(record))
  }
}
