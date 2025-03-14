/*

Accumulators let you add together data from all the tasks into a shared result.
Accumulators provide a mutable variable that a Spark cluster can safely update on a per-row basis.
For accumulator updates performed inside actions only, Spark guarantees that each task’s update to the accumulator will
be applied only once, meaning that restarted tasks will not update the value.
In transformations, you should be aware that each task’s update can be applied more than once if tasks or job stages
are re-executed.
Accumulators do not change the lazy evaluation model of Spark.
If an accumulator is being updated within an operation on an RDD, its value is updated only once that RDD is actually computed
(e.g., when you call an action on that RDD or an RDD that depends on it).

 */
package com.mahfooz.spark.sdv.accumulator

import com.mahfooz.spark.sdv.model.Flight
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Accumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass().getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val flightDF = spark.read
      .parquet(args(0))
      .as[Flight]

    val acc = spark.sparkContext.longAccumulator("China")

    def accChinaFunc(flight_row: Flight) = {
      val destination = flight_row.DEST_COUNTRY_NAME
      val origin = flight_row.ORIGIN_COUNTRY_NAME
      if (destination == "China") {
        acc.add(flight_row.count.toLong)
      }
      if (origin == "China") {
        acc.add(flight_row.count.toLong)
      }
    }

    val flights = flightDF.as[Flight]
    flights.foreach(flight_row => accChinaFunc(flight_row))

    println(acc.value)

  }
}
