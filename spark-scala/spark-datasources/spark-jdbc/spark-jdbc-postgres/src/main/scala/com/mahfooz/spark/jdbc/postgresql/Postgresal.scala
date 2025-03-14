/*

db {
  driver="org.postgresql.Driver"
  url="jdbc:postgresql://10.133.208.67:5444/CBF_TRANS"
  userName="hadoop_cbf"
  password="h@doop12#$"
}

 */
package com.mahfooz.spark.jdbc.postgresql

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Postgresal {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load("db.conf")
    val url = conf.getString("db.url")
    val userName = conf.getString("db.userName")
    val password = conf.getString("db.password")
    val driver = conf.getString("db.driver")

    println(
      "user:" + userName + "\nurl:" + url + "\npassword:" + password + "\ndriver:" + driver
    )

    val tablename = args(0)

    val spark = SparkSession
      .builder()
      .appName("Postgresal")
      .master("local")
      .getOrCreate()

    //Read from CSV file and save into postgres

    val dbDataFrame = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", userName)
      .option("password", password)
      .option("dbtable", tablename)
      .option("driver", driver)
      .option("truncate", false)
      .load()

    dbDataFrame.show(10)
  }
}
