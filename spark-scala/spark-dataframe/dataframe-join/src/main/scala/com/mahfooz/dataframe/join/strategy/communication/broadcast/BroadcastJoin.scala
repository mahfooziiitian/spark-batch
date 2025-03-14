package com.mahfooz.dataframe.join.strategy.communication.broadcast

import com.mahfooz.dataframe.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadcastJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(BroadcastJoin.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employeeDF = Seq(Employee("John", 31),
      Employee("Jeff", 33),
      Employee("Mary", 33),
      Employee("Mandy", 34),
      Employee("Julie", 34),
      Employee("Kurt", null.
        asInstanceOf[Int])
    ).toDF

    val deptDF = Seq(Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF

    println(s"table size for broadcast = ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")

    // print out the execution plan to verify broadcast hash join strategy is used
    employeeDF.join(
      broadcast(deptDF),
      employeeDF.col("dept_no") === deptDF.col("id")
    ).explain()

    deptDF.createOrReplaceTempView("departments")

    employeeDF.createOrReplaceTempView("employees")

    // using SQL
    spark.sql(
      """select /*+ MAPJOIN(departments) */ * from employees
        |JOIN departments on dept_no == id""".stripMargin).explain()

  }
}
