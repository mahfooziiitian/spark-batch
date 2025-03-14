/*
Inferring the Schema Using Reflection

Spark SQL supports two different methods for converting existing RDDs into Datasets.
The first method uses reflection to infer the schema of an RDD that contains specific types of objects.
This reflection-based approach leads to more concise code and works well when you already know the schema while writing your Spark application.


 */
package com.mahfooz.sparksql.schema.reflection

import com.mahfooz.sparksql.schema.model.Person
import org.apache.spark.sql.{Encoder, SparkSession}

object InferSchemaReflection {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val dataFile = if (args.length>0) args(0) else s"${sys.env("DATA_HOME").replace("\\","/")}/FileData/Text/people.txt"

    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile(dataFile)
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF =
      spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    teenagersDF
      .map(teenager => "Name: " + teenager.getAs[String]("name"))
      .show()

    implicit val mapEncoder: Encoder[Map[String, Any]] =
      org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF
      .map(teenager => teenager.getValuesMap[Any](List("name", "age")))
      .collect()
  }
}
