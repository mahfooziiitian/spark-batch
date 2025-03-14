package com.mahfooz.dataframe.join.types.natural

import org.apache.spark.sql.SparkSession

object NaturalJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("NaturalJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Abstrusest", 1, Seq(250, 100))
    ).toDF("person_id", "name", "program_id", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley")
    ).toDF("program_id", "degree", "department", "school")

    val naturalJoinDF = person.join(graduateProgram,"program_id")
    naturalJoinDF.explain()
    naturalJoinDF.show()

    //Using SQL
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")

    spark.sql(
      """
        |SELECT * FROM graduateProgram NATURAL JOIN person
        |""".stripMargin).show()

  }

}
