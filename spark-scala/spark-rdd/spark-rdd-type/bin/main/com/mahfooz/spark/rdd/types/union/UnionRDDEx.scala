/*

UnionRDD is the result of a union operation of two RDDs.
Union simply creates an RDD with elements from both RDDs.

class UnionRDD[T: ClassTag]( sc: SparkContext, var rdds: Seq[RDD[T]]) extends RDD[T](sc, Nil)

 */
package com.mahfooz.spark.rdd.types.union

import org.apache.spark.sql.SparkSession

object UnionRDDEx {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("UnionRDDEx")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd_one = sc.parallelize(Seq(1,2,3))
    val rdd_two = sc.parallelize(Seq(4,5,6))

    val unionRDD = rdd_one.union(rdd_two)
    unionRDD.take(10).foreach(element=>println(element))

  }
}
