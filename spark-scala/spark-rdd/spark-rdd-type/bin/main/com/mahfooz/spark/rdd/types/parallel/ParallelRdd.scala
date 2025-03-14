package com.mahfooz.spark.rdd.types.parallel

import org.apache.spark.sql.SparkSession

object ParallelRdd {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("ParallelRdd")
      .getOrCreate()

    val rdd=spark.sparkContext.parallelize(Seq(1,4,3,2,79,3,6,90))
    print(rdd)

  }

}
