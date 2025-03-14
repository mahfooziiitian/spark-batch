/*

Window aggregate functions (aka window functions or windowed aggregates) are functions that perform a calculation over a group of records
called window that are in some relation to the current record (i.e. can be in the same partition or frame as the current row).

Window functions are also called over functions due to how they are applied using over operator.

 */
package com.mahfooz.spark.window

import com.mahfooz.spark.model.Salary
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg

object SparkWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(SparkWindow.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val salaries = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)
    ).toDS

    //WindowSpec
    val byDepName = Window.partitionBy('depName)

    salaries
      .withColumn("salaries", avg('salary) over byDepName)
      .dropDuplicates()
      .show

  }

}
