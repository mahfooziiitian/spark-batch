package com.mahfooz.spark.rdd.caseclass

import org.apache.spark.sql.SparkSession

case class Contact(id: Long, name: String, email: String)

object CaseClassRdd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CaseClassRdd")
      .master("local[*]")
      .getOrCreate()

    val contactData =
      Array("1#John Doe#jdoe@domain.com", "2#Mary Jane#mjane@domain.com")
    val contactDataRDD = spark.sparkContext.parallelize(contactData)
    val contactRDD = contactDataRDD.map(l => {
      val contactArray = l.split("#")
      Contact(contactArray(0).toLong, contactArray(1), contactArray(2))
    })
    contactRDD.collect.foreach(println)
  }
}
