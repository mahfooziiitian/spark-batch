package com.mahfooz.sparksql.udf.register

import com.mahfooz.sparksql.udf.function.MyAverage
import org.apache.spark.sql.SparkSession

object ClassUdfRegister {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(ClassUdfRegister.getClass.getSimpleName)
      .getOrCreate()

    // Register the function to access it
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("D:\\data\\employee\\employees.json")
    df.createOrReplaceTempView("employees")

    df.show()
    val result =
      spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }

}
