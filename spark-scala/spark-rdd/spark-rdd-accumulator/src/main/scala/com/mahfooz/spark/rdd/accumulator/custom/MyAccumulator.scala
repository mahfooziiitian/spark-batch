package com.mahfooz.spark.rdd.accumulator.custom

import com.mahfooz.spark.rdd.accumulator.model.YearPopulation
import org.apache.spark.sql.SparkSession

object MyAccumulator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(MyAccumulator.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val statePopAcc = new StateAccumulator

    //Create a new StateAccumulator and register the same with SparkContext
    sc.register(statePopAcc, "statePopAcc")

    //Read the statesPopulation.csv as an RDD
    val statesPopulationRDD =
      sc.textFile("D:\\data\\spark\\csv\\statesPopulation.csv")
        .filter(_.split(",")(0) != "State")

    statesPopulationRDD.take(10).foreach(record => println(record))

    val count = statesPopulationRDD
      .map(x => {
        val tokens = x.split(",")
        val year = tokens(1).toInt
        val pop = tokens(2).toLong
        statePopAcc.add(YearPopulation(year, pop))
        x
      })
      .count

    print(statePopAcc)

  }
}
