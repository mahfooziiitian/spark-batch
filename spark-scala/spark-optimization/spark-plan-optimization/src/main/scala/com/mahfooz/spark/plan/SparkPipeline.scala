package com.mahfooz.spark.plan

import org.apache.spark.sql.SparkSession

object SparkPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-pipeline")
      .master("local[*]")
      .getOrCreate()

    val jump2Numbers = spark.range(0, 100000,2)
    val jump5Numbers = spark.range(0, 200000, 5)
    val ds1 = jump2Numbers.repartition(3)
    val ds2 = jump5Numbers.repartition(5)
    val joined = ds1.join(ds2,"id")
    joined.explain

    joined.show(100)

    System.in.read

  }

}
