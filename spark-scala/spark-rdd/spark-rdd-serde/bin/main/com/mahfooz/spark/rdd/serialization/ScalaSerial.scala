package com.mahfooz.spark.rdd.serialization

import com.mahfooz.spark.rdd.serialization.kryo.SomeClass
import org.apache.spark.sql.SparkSession

object ScalaSerial {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
        .appName("ScalaSerial")
        .master("local")
        .getOrCreate()

    val sc=spark.sparkContext
    sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))
  }
}
