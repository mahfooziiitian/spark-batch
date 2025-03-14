/*

./bin/spark-shell ../jdbc/mysql-connector-java-5.1.45/mysql-connectorjava-5.1.45-bin.jar \
  --jars ../jdbc/mysql-connector-java-5.1.45/mysqlconnector-java-5.1.45-bin.jar

import java.sql.DriverManager
val connectionURL = "jdbc:mysql://localhost:3306/<table>?user=<username>&password=<password>"
val connection = DriverManager.getConnection(connectionURL)
connection.isClosed()
connection close()

url The JDBC URL for Spark to connect to. At the minimum, it should contain the host, port,
and database name. For MySQL, it may look something like this: jdbc:mysql://
localhost:3306/sakila.
dbtable The name of a database table for Spark to read data from or write data to.
driver The class name of the JDBC driver that Spark will instantiate to connect to the previous
URL. Consult the JDBC driver documentation that you are using. For the MySQL
Connector/J driver, the class name is com.mysql.jdbc.Driver.

 */
package com.mahfooz.spark.df.ds.jdbc

import org.apache.spark.sql.SparkSession

object SparkJdbcReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkJdbcReader")
      .getOrCreate()

    val mysqlURL =
      "jdbc:mysql://mysqlmoveexternalapipoc.mysql.database.azure.com:3306/spark?useSSL=true"

    val filmDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", mysqlURL)
      .option("dbtable", "spark.offices")
      .option("user", "mysqlroot")
      .option("password", "Root@2021")
      .load()

    filmDF.printSchema()

    filmDF.select("city", "phone", "country").show(5)

  }
}
