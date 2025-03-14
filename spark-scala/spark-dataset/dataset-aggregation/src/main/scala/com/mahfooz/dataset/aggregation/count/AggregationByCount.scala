/*

def count(columnName: String): TypedColumn[Any, Long]
Aggregate function: returns the number of items in a group.

def count(e: Column): Column
Aggregate function: returns the number of items in a group.

def countDistinct(columnName: String, columnNames: String*): Column
Aggregate function: returns the number of distinct items in a group.

def countDistinct(expr: Column, exprs: Column*): Column
Aggregate function: returns the number of distinct items in a group.

 */
package com.mahfooz.dataset.aggregation.count

import org.apache.spark.sql.SparkSession

object AggregationByCount {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("AggregationByCount")
      .master("local[*]")
      .getOrCreate()


  }

}
