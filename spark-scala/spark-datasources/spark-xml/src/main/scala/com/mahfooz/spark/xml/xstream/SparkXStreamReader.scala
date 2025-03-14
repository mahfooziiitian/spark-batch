package com.mahfooz.spark.xml.xstream

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.DomDriver
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

case class Name(firstName: String, middleName: String, lastName: String)
case class Person(id: String, name: Name, ssn: String, gender: String, salary: String)

object SparkXStreamReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("spark-xstream-reader")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(Row("1", Row("James ", "", "Smith"), "36636", "M", "3000"),
      Row("2", Row("Michael ", "Rose", ""), "40288", "M", "4000"),
      Row("3", Row("Robert ", "", "Williams"), "42114", "M", "4000"),
      Row("4", Row("Maria ", "Anne", "Jones"), "39192", "F", "4000"),
      Row("5", Row("Jen", "Mary", "Brown"), "", "F", "-1")
    )
    val schema = new StructType()
      .add("id", StringType)
      .add("name", new StructType()
        .add("firstName", StringType)
        .add("middleName", StringType)
        .add("lastName", StringType))
      .add("ssn", StringType)
      .add("gender", StringType)
      .add("salary", StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)



    import spark.implicits._
    val dsPerson = df.as[Person]

    val dsString = dsPerson.mapPartitions(partition => {
      val xstream = new XStream(new DomDriver)
      val data = partition.map(person => {
        val xmlString = xstream.toXML(person)
        xmlString
      })
      data
    })

    val file_path = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Xml\\person.xml"
    dsString.write.text(file_path)

  }
}
