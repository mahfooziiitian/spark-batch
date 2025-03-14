package com.mahfooz.sparksql.schema.cases

import com.mahfooz.sparksql.schema.model.Person
import org.apache.spark.sql.SparkSession

object CaseSchema {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName("CaseSchema")
      .config("spark.master","local[*]")
      .getOrCreate()

    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    primitiveDS.show()

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = s"${sys.env("DATA_HOME")}/FileData/Json/students.json"
    val peopleDS = spark.read.json(path)
    peopleDS.show(truncate = false)
  }
}
