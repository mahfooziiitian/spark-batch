/*
Pivoting is a way to summarize the data by specifying one of the categorical columns and then performing aggregations on another
columns such that the categorical values are transposed from rows into individual columns.
Another way of thinking about pivoting is that it is a way to translate rows into columns while applying one or more aggregations.
This technique is commonly used in data analysis or reporting.
The pivoting process starts with the grouping of one or more columns, then pivots on a column, and finally ends with applying one or more aggregations on one or more
columns.

 */
package com.mahfooz.dataframe.columns.group

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

case class Student(
    name: String,
    gender: String,
    weight: Int,
    graduation_year: Int
)

object Pivoting {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(Pivoting.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val studentsDF = Seq(
      Student("John", "M", 180, 2015),
      Student("Mary", "F", 110, 2015),
      Student("Derek", "M", 200, 2015),
      Student("Julie", "F", 109, 2015),
      Student("Allison", "F", 105, 2015),
      Student("kirby", "F", 115, 2016),
      Student("Jeff", "M", 195, 2016)
    ).toDF

    // calculating the average weight for each gender per graduation year
    studentsDF.groupBy("graduation_year").pivot("gender").avg("weight").show()

    //The number of columns added after the group columns in the result table is
    //the product of the number of unique values of the pivot column and the number of
    //aggregations.
    studentsDF.groupBy("graduation_year").pivot("gender")
      .agg(
        min("weight").as("min"),
        max("weight").as("max"),
        avg("weight").as("avg")
      ).show()

    //Selecting Which Values of Pivoting Columns to Generate the
    //Aggregations For
    studentsDF.groupBy("graduation_year").pivot("gender", Seq("M"))
      .agg(
        min("weight").as("min"),
        max("weight").as("max"),
        avg("weight").as("avg")
      ).show()


  }
}
