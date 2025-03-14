/*

 */
package com.mahfooz.spark.collection.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CollectionJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkExplode")
      .getOrCreate()

    import spark.implicits._

    // create a string that contains JSON string
    val todos =
      """{"day": "Monday","tasks": 
        |["Pick Up John","Buy Milk","Pay Bill"]}""".stripMargin

    val todoStrDF = Seq((todos)).toDF("todos_str")

    // at this point, todoStrDF is DataFrame with one column of string type
    todoStrDF.printSchema

    // in order to convert a JSON string into a Spark struct data type, we need to describe its structure to Spark
    val todoSchema = new StructType()
      .add("day", StringType)
      .add("tasks", ArrayType(StringType))

    // use from_json to convert JSON string
    val todosDF =
      todoStrDF.select(from_json('todos_str, todoSchema).as("todos"))

    // todos is a struct data type that contains two fields: day and tasks
    todosDF.printSchema

    // retrieving value out of struct data type using the getItem function of Column class
    todosDF
      .select(
        'todos.getItem("day"),
        'todos.getItem("tasks"),
        'todos.getItem("tasks").getItem(0).as("first_task")
      )
      .show(false)

    // to convert a Spark struct data type to JSON string, we can use to_json function
    todosDF.select(to_json('todos)).show(false)
  }
}
