package com.mahfooz.dataframe.join.duplicate

import org.apache.spark.sql.SparkSession

object DropColumnAfterJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("DropColumnAfterJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Abstrusest", 1, Seq(250, 100))
    ).toDF("person_id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley")
    ).toDF("id", "degree", "department", "school")

    val gradProgramDupe = graduateProgram
      .withColumnRenamed("id", "graduate_program")

    val joinExpr =
      gradProgramDupe.col("graduate_program") === person.col("graduate_program")

    val joinedDF = person
      .join(gradProgramDupe, joinExpr)
      .drop(person.col("graduate_program"))
      .select("*")
    joinedDF.explain()
    joinedDF.show()

  }

}
