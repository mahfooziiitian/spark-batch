/*

CoGroupedRDD is an RDD that co-groups its parents.
Both parent RDDs have to be pairRDDs for this to work, as a co-group essentially generates a
pairRDD consisting of the common key and list of values from both parent RDDs.

class CoGroupedRDD[K] extends RDD[(K, Array[Iterable[_]])]

 */
package com.mahfooz.spark.rdd.types.cogroup

import org.apache.spark.sql.SparkSession

object CoGroupRddEx {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CoGroupRddEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val statesPopulationRDD =
      sc.textFile("D:\\data\\spark\\csv\\statesPopulation.csv")

    val pairRDD = statesPopulationRDD.map(
      record => (record.split(",")(0), record.split(",")(2))
    )

    val pairRDD2 = statesPopulationRDD.map(
      record => (record.split(",")(0), record.split(",")(1)))

    val coGroupRDD = pairRDD.cogroup(pairRDD2)

    coGroupRDD.foreach(record=>println(record))
  }
}
