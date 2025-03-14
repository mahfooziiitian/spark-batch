package com.mahfooz.dataframe.join.types.equijoin

import org.apache.spark.sql.SparkSession

object EquiJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("EquiJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Abstrusest", 1, Seq(250, 100))
    ).toDF("person_id", "name", "graduate_id", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley")
    ).toDF("program_id", "degree", "department", "school")

    val equiJoin = person.join(graduateProgram,
      person.col("graduate_id") === graduateProgram.col("program_id"))
    equiJoin.show()
    equiJoin.explain()
  }

}
