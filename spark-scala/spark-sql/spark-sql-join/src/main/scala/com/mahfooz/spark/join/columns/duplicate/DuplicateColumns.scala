/*

Sometimes there is an unexpected issue that comes up after joining two DataFrames with one or more columns that have the same name.
When this happens, the joined DataFrame would have multiple columns with the same name.
In this situation, it is not easy to refer to one of those columns while performing some kind of transformation on the joined DataFrame.

 */
package com.mahfooz.spark.join.columns.duplicate

import com.mahfooz.spark.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object DuplicateColumns {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("DuplicateColumns")
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

    // add a new column to deptDF with name dept_no
    val deptDF2 = deptDF.withColumn("dept_no", 'id)
    deptDF2.printSchema

    // now employeeDF with deptDF2 using dept_no column
    val dupNameDF = employeeDF.join(
      deptDF2,
      employeeDF.col("dept_no") === deptDF2.col("dept_no")
    )
    dupNameDF.printSchema

    //dupNameDF.select("dept_no")
    //duplicate column error

    //Sol1: Use the Original DataFrame
    dupNameDF.select(deptDF2.col("dept_no"))

    //Renaming Column Before Joining
  }
}
