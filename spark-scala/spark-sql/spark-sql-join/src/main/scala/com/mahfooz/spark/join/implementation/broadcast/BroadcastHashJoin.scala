package com.mahfooz.spark.join.implementation.broadcast
import com.mahfooz.spark.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadcastHashJoin {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("BroadcastHashJoin")
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

    // register them as views so we can use SQL for perform joins
    employeeDF.createOrReplaceTempView("employees")
    deptDF.createOrReplaceTempView("departments")

    // print out the execution plan to verify broadcast hash join strategy is used
    employeeDF
      .join(broadcast(deptDF),
        employeeDF.col("dept_no") === deptDF.col("id"))
      .explain()

    // using SQL
    spark
      .sql(
        """select /*+ MAPJOIN(departments) */ * from employees
          |JOIN departments on dept_no == id""".stripMargin
      )
      .explain()

    spark
      .sql(
        """select /*+ BROADCAST(departments) */ * from employees
          |JOIN departments on dept_no == id""".stripMargin
      )
      .explain()

    println(s"spark.sql.autoBroadcastJoinThreshold = ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")}")

  }
}
