package com.mahfooz.spark.dataframe.creating

import org.apache.spark.sql.SparkSession

import scala.util.Random

object CreatingDataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CreatingDataFrame")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc
      .parallelize(1 to 10)
      .map(x => (x, Random.nextInt(100) * x))

    import spark.implicits._

    val kvDF = rdd.toDF("key", "value")

    kvDF.show

    kvDF.printSchema()
  }
}
