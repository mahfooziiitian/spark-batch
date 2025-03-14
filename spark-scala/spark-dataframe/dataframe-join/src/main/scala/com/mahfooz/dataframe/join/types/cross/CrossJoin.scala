package com.mahfooz.dataframe.join.types.cross

import com.mahfooz.dataframe.join.model.{Dept, Employee}
import org.apache.spark.sql.SparkSession

object CrossJoin {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(CrossJoin.getClass.getSimpleName)
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

    // using crossJoin transformation and display the count
    val crossJoin = employeeDF.crossJoin(deptDF)
    crossJoin.explain()
    println(crossJoin.count)
    crossJoin.show(truncate = false, numRows = 100)
  }

}
