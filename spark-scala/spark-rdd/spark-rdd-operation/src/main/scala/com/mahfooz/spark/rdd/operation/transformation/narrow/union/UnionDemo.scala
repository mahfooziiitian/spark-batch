package com.mahfooz.spark.rdd.operation.transformation.narrow.union

import org.apache.spark.sql.SparkSession

object UnionDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName(UnionDemo.getClass.getName)
      .getOrCreate()

    val rdd1 = spark.sparkContext.parallelize(
      Seq((1, "jan", 2016), (3, "nov", 2014), (16, "feb", 2014))
    )
    val rdd2 =
      spark.sparkContext.parallelize(Seq((5, "dec", 2014), (17, "sep", 2015)))
    val rdd3 =
      spark.sparkContext.parallelize(Seq((6, "dec", 2011), (16, "may", 2015)))
    val rddUnion = rdd1.union(rdd2).union(rdd3)
    rddUnion.foreach(println)

  }
}
