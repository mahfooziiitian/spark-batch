package com.mahfooz.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object DatasetEx {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("DatasetEx").master("local[*]").getOrCreate()
    val numDS=spark.range(5,50,5)
    numDS.show()
    numDS.orderBy(desc("id")).show(5)
    numDS.describe()
  }

}
