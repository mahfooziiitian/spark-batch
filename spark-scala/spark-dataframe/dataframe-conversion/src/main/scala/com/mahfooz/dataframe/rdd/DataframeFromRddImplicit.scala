package com.mahfooz.dataframe.rdd

import scala.util.Random
import org.apache.spark.sql.SparkSession

object DataframeFromRddImplicit {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeFromRddImplicit")
      .getOrCreate()

    val rdd = spark.sparkContext
      .parallelize(1 to 10)
      .map(x => (x, Random.nextInt(100) * x))
    import spark.implicits._
    val kvDF = rdd.toDF("key", "value")
    kvDF.show(10)
  }
}
