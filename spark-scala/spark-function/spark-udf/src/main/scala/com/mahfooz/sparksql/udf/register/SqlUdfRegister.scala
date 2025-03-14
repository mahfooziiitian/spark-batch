package com.mahfooz.sparksql.udf.register

import com.mahfooz.spark.model.Student
import com.mahfooz.sparksql.udf.function.SimpleFunc
import org.apache.spark.sql.SparkSession

object SqlUdfRegister {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SqlUdfRegister")
      .getOrCreate()

    import spark.implicits._

    val studentDF =
      Seq(Student("Joe", 85), Student("Jane", 90), Student("Mary", 55)).toDF()

    // register as a view
    studentDF.createOrReplaceTempView("students")

    spark.sqlContext.udf.register("letterGrade", SimpleFunc.letterGrade(_: Int): String)
    spark
      .sql("select name, score, letterGrade(score) as grade from students")
      .show
  }
}
