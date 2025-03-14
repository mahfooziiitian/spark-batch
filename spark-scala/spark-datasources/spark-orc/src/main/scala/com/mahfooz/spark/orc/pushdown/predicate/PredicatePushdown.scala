package com.mahfooz.spark.orc.pushdown.predicate

import com.mahfooz.spark.orc.model.{Contact, Person}
import org.apache.spark.sql.{SaveMode, SparkSession}

object PredicatePushdown {

  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\orc"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("predicate-pushdown")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val records = (1 to 100).map { i =>
      Person(s"name_$i", i, (0 to 1).map { m => Contact(s"contact_$m", s"phone_$m") })
    }

    val dataFile = s"${dataHome}\\peoplePartitioned"
    records.toDF().write.format("orc")
      .mode(SaveMode.Overwrite)
      .partitionBy("age")
      .save(dataFile)

    spark.sql("set spark.sql.orc.filterPushdown=true")

    val people = spark.read.format("orc").load(dataFile)
    people.filter(people("age") < 15).select("name").explain(true)
  }

}
