package com.mahfooz.spark.dataframe.row.sampling

import org.apache.spark.sql.SparkSession

object RowSampling {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load(args(0))

    val seed = 5
    val withReplacement = false
    val fraction = 0.5

    println(df.sample(withReplacement, fraction, seed).count())

  }
}
