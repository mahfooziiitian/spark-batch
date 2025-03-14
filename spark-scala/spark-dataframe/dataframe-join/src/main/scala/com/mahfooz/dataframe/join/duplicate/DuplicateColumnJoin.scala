package com.mahfooz.dataframe.join.duplicate

import com.mahfooz.dataframe.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object DuplicateColumnJoin {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(DuplicateColumnJoin.getClass.getSimpleName)
      .master("local[*]")
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

    // add a new column to deptDF with name dept_no
    val deptDF2 = deptDF.withColumn("dept_no", 'id)
    deptDF2.printSchema

    // now employeeDF with deptDF2 using dept_no column
    val dupNameDF = employeeDF.join(deptDF2, "dept_no")
    dupNameDF.explain()
    dupNameDF.show()

    dupNameDF.select("dept_no").show()

  }

}
