package com.mahfooz.dataframe.optimization.catalyst

import org.apache.spark.sql.SparkSession

// Business object
case class Person(id: String, number: String, eDad: Int)


object CatalystOptimizer {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder().appName("catalyst-optimizer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // The dataset to query
    val peopleDataset  = Seq(
      Person("001", "Bob", 28),
      Person("002", "Joe", 34)).toDS

    // The query to execute
    val query = peopleDataset.groupBy("number").count().as("total")

    // Get Catalyst optimization plan
    query.explain(extended = true)

  }

}
