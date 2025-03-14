/*

Stages in Spark represent groups of tasks that can be executed together to compute the same operation on multiple machines.
In general, Spark will try to pack as much work as possible (i.e., as many transformations as possible inside your job) into
the same stage, but the engine starts new stages after operations called shuffles.


 */
package com.mahfooz.spark.job.stages

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkStages {

  private def simpleSparkProgram(rdd : RDD[Int]): Long ={
    //stage1
    rdd.filter(_< 10)
      .map(x => (x, x) )
      //stage2
      .groupByKey()
      .map{ case(value, groups) => (groups.sum, value)}
    //stage 3
    .sortByKey()
      .count()
  }
  def main(args: Array[String]): Unit = {
    // Create a Spark configuration
    val conf = new SparkConf().setAppName("RDDStagesExample").setMaster("local[*]")

    // Create a Spark context
    val sc = new SparkContext(conf)

    // Sample data
    val data = List(1, 2, 3, 4, 5, 89, 20, 40, 9, 30, 7)

    // Create an RDD
    val rdd = sc.parallelize(data)

    print(simpleSparkProgram(rdd))

    Thread.sleep(2000000)
  }

}
