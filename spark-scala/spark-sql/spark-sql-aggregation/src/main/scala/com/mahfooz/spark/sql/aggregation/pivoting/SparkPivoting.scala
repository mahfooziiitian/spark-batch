/*

Pivoting is a great way of transforming the table to create a different view, more suitable to doing many
summarizations and aggregations.
This is accomplished by taking the values of a column and making each of the values an actual column.


SELECT * FROM (
  SELECT year(date) year, month(date) month, temp
  FROM high_temps
  WHERE date BETWEEN DATE '2015-01-01' AND DATE '2018-08-31'
)
PIVOT (
  CAST(avg(temp) AS DECIMAL(4, 1))
  FOR month in (
    1 JAN, 2 FEB, 3 MAR, 4 APR, 5 MAY, 6 JUN,
    7 JUL, 8 AUG, 9 SEP, 10 OCT, 11 NOV, 12 DEC
  )
)
ORDER BY year DESC

 */
package com.mahfooz.spark.sql.aggregation.pivoting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Student(
    name: String,
    gender: String,
    weight: Int,
    graduation_year: Int
)

object SparkPivoting {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkPivoting")
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
    studentsDF
      .groupBy("graduation_year")
      .pivot("gender")
      .avg("weight")
      .show()

    studentsDF.createOrReplaceTempView("student_df")

    spark.sql("" +
      "SELECT * FROM student_df" +
      // group by graduation_year" +
      " PIVOT (min(weight) AS min, max(weight) AS max," +
      " avg(weight) AS avg" +
      " FOR gender IN ('M' AS Male, 'F' AS Female))")
      .show()

    //Multiple Aggregations After Pivoting
    studentsDF
      .groupBy("graduation_year")
      .pivot("gender")
      .agg(
        min("weight").as("min"),
        max("weight").as("max"),
        avg("weight").as("avg")
      )
      .show()
  }
}
