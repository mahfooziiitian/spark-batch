/*

spark-shell ../jdbc/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar  \
  --jars ../jdbc/mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar

 */

package com.mahfooz.spark.jdbc.shell
import java.sql.DriverManager

object SparkShellJdbc {

  def main(args: Array[String]): Unit = {
    val connectionURL =
      "jdbc:mysql://localhost:3306/spark?user=root&password=root"
    val connection = DriverManager.getConnection(connectionURL)
    connection.isClosed()
    connection close ()
  }
}
