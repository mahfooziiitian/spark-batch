package com.mahfooz.dataframe.join.types.antileft

import com.mahfooz.dataframe.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object AntiLeftJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(AntiLeftJoin.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employeeDF = Seq(
      Employee("John", 31),
      Employee("Jeff", 33),
      Employee("Mary", 33),
      Employee("Mandy", 34),
      Employee("Julie", 34),
      Employee("Kurt", null.asInstanceOf[Int])
    ).toDF

    val deptDF = Seq(
      Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF

    val joinedDF = employeeDF.join(
      deptDF,
      'dept_no === 'id,
      "left_anti"
    )
    joinedDF.explain()
    joinedDF.show()

  }
}
