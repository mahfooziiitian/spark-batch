package com.mahfooz.spark.sdv.accumulator.custom

import com.mahfooz.spark.sdv.model.Flight
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object EvenAccumulatorTest {

  def main(args: Array[String]): Unit = {

    val arr = ArrayBuffer[BigInt]()

    val spark=SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val acc = new EvenAccumulator
    val newAcc = sc.register(acc, "evenAcc")

    import spark.implicits._

    val flightDF = spark.read
      .parquet(args(0))
      .as[Flight]

    val flights = flightDF.as[Flight]
    flights.foreach(flight_row => acc.add(flight_row.count))
    println(acc.value)
  }
}
