package com.mahfooz.spark.join.cartesian

import com.mahfooz.spark.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object CartesianJoin {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("CartesianJoin")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val employeeDF = Seq(
      Employee("John", 31),
      Employee("Jeff", 33),
      Employee("Mary", 33),
      Employee("Mandy", 34),
      Employee("Julie", 34),
      Employee("Mahfooz", 1),
      Employee("Kurt", null.asInstanceOf[Int])
    ).toDF

    employeeDF.printSchema()

    val deptDF = Seq(
      Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF

    deptDF.printSchema()

    // register them as views so we can use SQL for perform joins
    employeeDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")

    // using crossJoin transformation and display the count
    println(employeeDF.crossJoin(deptDF).count)

    // using SQL and to display up to 30 rows to see all rows in the joined dataset
    spark.sql("select * from employees CROSS JOIN departments").show(30)
  }
}
