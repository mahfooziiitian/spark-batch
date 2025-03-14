package com.mahfooz.dataframe.join.complex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object JoinOnArrayType {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("JoinOnArrayType")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Abstrusest", 1, Seq(250, 100))
    ).toDF("id", "name", "program_id", "spark_status")

    val sparkStatus = Seq(
      (100, "In Progress"),
      (250, "Completed"),
      (500, "Pending")
    ).toDF("id", "status")

    val joinExpression = expr("array_contains(spark_status, id)")

    person
      .withColumnRenamed("id", "personId")
      .join(sparkStatus,joinExpression ).explain()

    person
      .withColumnRenamed("id", "personId")
      .join(sparkStatus, joinExpression).show()
  }
}
