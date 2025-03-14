package com.mahfooz.df.aggregation.groupby.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, expr}

object SparkJoinGroupByOrder {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("spark-join-group-order")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.range(1,10000000).toDF("id")
      .withColumn("odd_even",expr("id % 2"))

    val df2 = spark.range(1,80000).toDF("id")
      .withColumn("odd_even",expr("id % 3"))

    val df3= df1.join(df2, "id")
      .groupBy(df1("odd_even"))
      .agg(count("*").as("count"))

    df3.explain()

    df3.show(false)

    scala.io.StdIn.readLine()

  }

}
