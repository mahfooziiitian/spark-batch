package com.mahfooz.spark.rdd.partition.partitioners.custom

import org.apache.spark.sql.SparkSession

object CustomPartitionerMain {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(CustomPartitionerMain.getClass.getName)
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd=sc.parallelize(Seq("Poland","Germany","United Kingdom","India","Pakistan"))
      .map(record=> (record,record.length))
      .partitionBy(new CustomPartitioner(5))

    println("Default parallelism: "+sc.defaultParallelism)
    println("Number of partitions: "+rdd.getNumPartitions)
    println("Partitioner: "+rdd.partitioner)

    //It return an RDD created by coalescing all elements within each partition into a list.
    rdd.glom().collect().foreach(record=>println(record.mkString(",")))

  }

}
