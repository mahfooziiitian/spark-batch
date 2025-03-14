package com.mahfooz.datafram.aggregation.pivot

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object PivotWithSameColumn {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PivotWithSameColumn")
      .getOrCreate()

    import spark.implicits._

    //spark.sql.pivotMaxValues (default: 10000) controls the maximum number of (distinct) values that will be collected without error
    println(spark.conf.get("spark.sql.pivotMaxValues"))

    val pivotQ = spark.
      range(10).
      withColumn("group", 'id % 2).
      groupBy("group").
      pivot("group").
      agg(count("id"))

    println(pivotQ.queryExecution.logical.numberedTreeString)

    pivotQ.show(false)

    pivotQ.explain()
  }

}
