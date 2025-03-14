package com.mahfooz.spark.rdd.partition.dependencies

import org.apache.spark.sql.SparkSession

object RddDependencies{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("RddDependencies").getOrCreate()

    // Sample data
    val data = Seq(1, 2, 3, 4, 5)

    // Create an RDD
    val rdd = spark.sparkContext.parallelize(data)

    // Transformations
    val squared_rdd = rdd.map(x => x * x)
    val filtered_rdd = squared_rdd.filter(x => x > 5)

    //Dependencies
    val dependencies_squared = squared_rdd.dependencies
    val dependencies_filtered = filtered_rdd.dependencies

    // Print the dependencies
    print("Dependencies of squared_rdd:", dependencies_squared)
    print("Dependencies of filtered_rdd:", dependencies_filtered)

    //Stop the Spark context
    spark.stop()
  }
}