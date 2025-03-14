/*

Create an RDD of Rows from the original RDD.
Create the schema represented by a StructType matching the structure of Rows in the RDD.
Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.

 */
package com.mahfooz.spark.rdd.dataframe.program
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class RddToDfProgramatically {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .getOrCreate()

    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    import spark.implicits._

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()

  }
}
