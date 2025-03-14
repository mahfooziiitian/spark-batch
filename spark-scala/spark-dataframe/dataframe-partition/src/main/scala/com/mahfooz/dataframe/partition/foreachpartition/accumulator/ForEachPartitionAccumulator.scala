package com.mahfooz.dataframe.partition.foreachpartition.accumulator

import org.apache.spark.sql.SparkSession

object ForEachPartitionAccumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ForEachPartitionAccumulator")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9))
    val longAcc = spark.sparkContext.longAccumulator("sumAccumulator")
    rdd.foreachPartition(partition=> {
      longAcc.add(partition.length)
    })

    println("Accumulator value:"+longAcc.value)

  }

}
