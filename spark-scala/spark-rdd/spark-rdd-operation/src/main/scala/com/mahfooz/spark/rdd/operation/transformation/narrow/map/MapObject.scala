package com.mahfooz.spark.rdd.operation.transformation.narrow.map

import org.apache.spark.sql.SparkSession

case class Contact(id: Long, name: String, email: String)

object MapObject {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("MapObject")
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val contactData =
      Array("1#John Doe#jdoe@domain.com", "2#Mary Jane#mjane@domain.com")
    val contactDataRDD = sc.parallelize(contactData)
    val contactRDD = contactDataRDD.map(l => {
      val contactArray = l.split("#")
      Contact(contactArray(0).toLong, contactArray(1), contactArray(2))
    })
    contactRDD.collect.foreach(println)
  }
}
