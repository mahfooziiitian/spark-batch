package com.mahfooz.sparksql.udf.register

import com.mahfooz.spark.model.Student
import com.mahfooz.sparksql.udf.function.SimpleFunc.letterGrade
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkUdfRegister {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkUdfRegister")
      .getOrCreate()

    import spark.implicits._

    val studentDF =
      Seq(Student("Joe", 85), Student("Jane", 90), Student("Mary", 55)).toDF()


    //register as a UDF
    val letterGradeUDF = udf(letterGrade(_: Int): String)

    // use the UDF to convert scores to letter grades
    studentDF
      .select($"name", $"score", letterGradeUDF($"score").as("grade"))
      .show
  }

}
