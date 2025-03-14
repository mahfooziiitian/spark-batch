package com.mahfooz.spark.join.expression

import org.apache.spark.sql.SparkSession

object ComplexJoinExpression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ComplexJoinExpression")
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

    person.createOrReplaceTempView("person")
    sparkStatus.createOrReplaceTempView("spark_status")

    spark.sql("""
        |WITH person_modified AS (
        | select
        |   id as personId,
        |   name,
        |   program_id as graduate_program,
        |   spark_status
        |FROM person)
        |SELECT * FROM
        |     person_modified
        |     INNER JOIN spark_status ON array_contains(spark_status, id)
        |""".stripMargin).show(false)
  }
}
