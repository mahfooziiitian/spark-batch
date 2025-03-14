/*
Broadcast variables are shared variables across all executors.
Broadcast variables are created once in the Driver and then are read only on executors.
Entire datasets can be broadcast in a Spark cluster so that executors have access to the broadcast data.
All the tasks running within an executor all have access to the broadcast variables.
Broadcast uses various optimized methods to make the broadcast data accessible to all executors.
The driver can only broadcast the data it has and you cannot broadcast RDDs by using references.
This is because only Driver knows how to interpret RDDs and executors only know the particular partitions of
data they are handling.

Broadcast variables can be both created and destroyed too.

 */
package com.mahfooz.spark.rdd.broadcast

import org.apache.spark.sql.SparkSession

object RDDBroadcast {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(RDDBroadcast.getClass.getName)
      .master("local[3]")
      .getOrCreate()

    val states =
      Map(("NY", "New York"), ("CA", "California"), ("FL", "Florida"))
    val countries = Map(("USA", "United States of America"), ("IN", "India"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(
      ("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )

    val rdd = spark.sparkContext.parallelize(data)

    val rdd2 = rdd.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (f._1, f._2, fullCountry, fullState)
    })

    println(rdd2.collect().mkString("\n"))
  }
}
