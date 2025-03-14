/*
Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from
the left DataFrame as well as any rows in the right DataFrame that have a match in the left
DataFrame.

If there is no equivalent row in the right DataFrame, Spark will insert null:

As expected, the number of rows in the joined dataset is the same as the number
of rows in the employee DataFrame.

Since there is no matching department with an ID value of 0, it fills in a NULL value for that row.

The result of this particular left outer join enables you to tell which department an employee is assigned to as well as which
employees are not assigned to a department.

 */
package com.mahfooz.dataframe.join.types.outer.left

import com.mahfooz.dataframe.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object LeftOuterJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(LeftOuterJoin.getClass.getSimpleName)
      .master("local")
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

    val joinType = "left_outer"
    val joinExpression = 'dept_no === 'id

    // the join type can be either "left_outer" or "leftouter"
    employeeDF.join(deptDF, joinExpression, joinType).printSchema()
    /*
    SQL equivalent:
    SELECT * FROM employeeDF LEFT OUTER JOIN deptDF
    ON employeeDF.dept_no = deptDF.id
     */

    employeeDF.join(deptDF, joinExpression, joinType).show(false)

    employeeDF
      .join(deptDF, joinExpression, joinType)
      .where('dept_no === 33)
      .select("dept_no", "deptName", "name")
      .explain()

    employeeDF
      .join(deptDF, joinExpression, joinType)
      .select("dept_no", "deptName", "name")
      .where('dept_no === 33)
      .explain()

  }
}
