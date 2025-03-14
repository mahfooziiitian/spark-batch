/*

The join expression can be specified inside the join transformation or using the where transformation.

 */
package com.mahfooz.spark.join

import com.mahfooz.spark.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object SparkJoin {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkJoin")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
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

    // define the join expression of equality comparison
    val joinExpression = employeeDF.col("dept_no") === deptDF.col("id")

    // perform the join
    employeeDF.join(deptDF, joinExpression, "inner").show

    // no need to specify the join type since "inner" is the default
    employeeDF.join(deptDF, joinExpression).show
    // specify the join expression inside the join transformation
    employeeDF.join(deptDF, employeeDF.col("dept_no") === deptDF.col("id")).show

    // a shorter version of the join expression
    employeeDF.join(deptDF, 'dept_no === 'id).show

    // specify the join expression using the where transformation
    employeeDF.join(deptDF).where('dept_no === 'id).show

    // using SQL
    spark.sql("select * from employees JOIN departments on dept_no == id").show

  }
}
