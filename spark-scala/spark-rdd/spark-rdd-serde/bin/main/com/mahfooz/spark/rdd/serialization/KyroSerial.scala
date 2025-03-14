package com.mahfooz.spark.rdd.serialization

import com.mahfooz.spark.rdd.serialization.kryo.SomeClass
import org.apache.spark.{SparkConf, SparkContext}

object KyroSerial {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("KyroSerial")

    conf.registerKryoClasses(Array(classOf[SomeClass]))
    val sc = new SparkContext(conf)

    sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))
  }
}
