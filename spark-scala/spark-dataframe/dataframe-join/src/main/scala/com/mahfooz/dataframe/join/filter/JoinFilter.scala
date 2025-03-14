package com.mahfooz.dataframe.join.filter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object JoinFilter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(JoinFilter.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val emp = Seq(
      (1, "Smith", 1, "10", 3000),
      (2, "Rose", 1, "20", 4000),
      (3, "Williams", 1, "10", 1000),
      (4, "Jones", 2, "10", 2000),
      (5, "Brown", 2, "40", -1),
      (6, "Brown", 2, "50", -1)
    )

    val empColumns = Seq("emp_id", "name", "superior_emp_id", "emp_dept_id", "salary")

    val empDF = emp.toDF(empColumns: _*)

    val selfJoinDF = empDF.as("emp1")
      .join(empDF.as("emp2")).
      where(
        col("emp1.superior_emp_id") === col("emp2.emp_id")
      ).
      select(
        col("emp1.emp_id"),
        col("emp1.name"),
        col("emp2.emp_id").as("superior_emp_id"),
        col("emp2.name").as("superior_emp_name")
      )

    selfJoinDF.explain
    selfJoinDF.show()
  }

}
