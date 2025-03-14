/*

The behavior of this join type is similar to the inner join type, except the joined dataset doesn't include the columns from the right
dataset.
Another way of thinking about this join type is its behavior is the opposite of the left anti-join, where the joined dataset contains only
the matching rows.

 */
package com.mahfooz.spark.join.semi.left

import com.mahfooz.spark.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object LeftSemiJoin {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("LeftSemiJoin")
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

    employeeDF.join(deptDF, 'dept_no === 'id, "left_semi").show

    // using SQL
    spark
      .sql(
        "select * from employees LEFT SEMI JOIN departments on dept_no == id"
      )
      .show

  }
}
