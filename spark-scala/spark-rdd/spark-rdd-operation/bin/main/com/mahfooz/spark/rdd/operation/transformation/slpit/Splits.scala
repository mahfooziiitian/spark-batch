package com.mahfooz.spark.rdd.operation.transformation.split

import org.apache.spark.sql.SparkSession

object Splits {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName ("Splits")
      .master ("local")
      .getOrCreate ()

    val sc=spark.sparkContext

    val lines = sc.textFile (getClass
      .getResource("/spark_test.txt").getFile)

    val words=lines.flatMap(line => line.split(" "))

    val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

    fiftyFiftySplit.foreach(println)
  }

}
