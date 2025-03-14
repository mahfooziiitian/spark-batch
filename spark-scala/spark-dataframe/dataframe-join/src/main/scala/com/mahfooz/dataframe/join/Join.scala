/*
Performing a join of two datasets requires you to specify two pieces of information.

a)  The first one is a join expression that specifies which columns from each dataset should be used to determine which rows from both
    datasets will be included in the joined dataset.

b)  The second one is the join type, which determines what should be included in the joined dataset.

 */
package com.mahfooz.dataframe.join

import com.mahfooz.dataframe.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object Join {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(Join.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val employeeDF = Seq( Employee("John", 31),
      Employee("Jeff", 33),
      Employee("Mary", 33),
      Employee("Mandy", 34),
      Employee("Julie", 34),
      Employee("Kurt", null.
        asInstanceOf[Int])
    ).toDF

    val deptDF = Seq( Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF

    // register them as views so we can use SQL for perform joins
    employeeDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")


  }

}
