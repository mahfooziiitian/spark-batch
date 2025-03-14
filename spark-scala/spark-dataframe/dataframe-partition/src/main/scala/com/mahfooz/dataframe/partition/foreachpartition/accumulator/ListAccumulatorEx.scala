package com.mahfooz.dataframe.partition.foreachpartition.accumulator

import org.apache.spark.sql.SparkSession


object ListAccumulatorEx {

  def main(args: Array[String]): Unit = {

   val spark= SparkSession
     .builder()
     .appName("list-accumulator")
     .master("local[*]")
     .getOrCreate()



  }

}
