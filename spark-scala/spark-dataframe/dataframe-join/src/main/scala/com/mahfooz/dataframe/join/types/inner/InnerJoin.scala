package com.mahfooz.dataframe.join.types.inner

import org.apache.spark.sql.SparkSession

object InnerJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("InnerJoin")
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

    val joinExpression = person.col("graduate_id") === graduateProgram.col(
      "program_id"
    )

    // perform the join
    person.join(graduateProgram, joinExpression, "inner").show

    // no need to specify the join type since "inner" is the default
    person.join(graduateProgram, joinExpression).explain(true)
    person.join(graduateProgram, joinExpression).show

    // a shorter version of the join expression
    person.join(graduateProgram, 'graduate_id === 'program_id).show

    // specify the join expression inside the join transformation
    val equiJoinDF = person
      .join(
        graduateProgram,
        person.col("graduate_id") === graduateProgram.col("program_id")
      )

    equiJoinDF.explain()
    equiJoinDF.show

    // specify the join expression using the where transformation
    val joinDFWhere = person.join(graduateProgram).where('graduate_id === 'program_id)

    joinDFWhere.explain()
    joinDFWhere.show

  }
}
