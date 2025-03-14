/*

spark.read.jdbc
spark.read.format("jdbc")

url
  The JDBC URL for Spark to connect to.
  At the minimum, it should contain the host, port, and database name.
  For MySQL, it may look something like this: jdbc:mysql://localhost:3306/sakila.

dbtable
  The name of a database table for Spark to read data from or write data to.

driver
  The class name of the JDBC driver that Spark will instantiate to connect to the previous URL.
  For the MySQL Connector/J driver, the class name is com.mysql.jdbc.Driver.

.option("user", "<username>")                                      
.option("password","<password>")

 */
package com.mahfooz.spark.jdbc

import org.apache.spark.sql.SparkSession

object ReadFromDb {

  def main(args: Array[String]): Unit = {

    val driver = "org.sqlite.JDBC"
    val path = "e:/database/sqlite/db/sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tablename = "flight_info"

    val spark = SparkSession
      .builder()
      .appName("ReadFromDb")
      .master("local")
      .getOrCreate()

    val dbDataFrame = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tablename)
      .option("driver", driver)
      .load()

    dbDataFrame.show()

    spark.stop()
  }
}
