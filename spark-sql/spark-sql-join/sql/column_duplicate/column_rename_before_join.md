package com.mahfooz.spark.join.columns.duplicate

import com.mahfooz.spark.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object ColumnRenameBeforeJoin {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("ColumnRenameBeforeJoin")
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
    val deptDF2 = deptDF.withColumnRenamed("id", "dept_no_d")
    deptDF2.printSchema

    // now employeeDF with deptDF2 using dept_no column
    val dupNameDF = employeeDF.join(
      deptDF2,
      employeeDF.col("dept_no") === deptDF2.col("dept_no_d")
    )
    dupNameDF.printSchema
    dupNameDF.select("dept_no")

  }
}
