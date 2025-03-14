/*
Reading the csv file and loading data into postgres database.
 */
package com.mahfooz.spark.jdbc.postgresql.load

import com.mahfooz.spark.jdbc.postgresql.common.SparkCommon
import org.apache.spark.sql.SaveMode

object LoadCsvFile {

  def main(args: Array[String]): Unit = {
    val sparkSession=SparkCommon.createSparkSession("LoadCsvFile")

    //1. Reading CSV file
    val csvFileDF=sparkSession.read
      .option("header",true)
      .option("delimiter",",")
      .option("inferSchema",true)
      .option("dateFormat", "YYYY-MM-dd")
      .csv("D:\\data\\database\\postgres\\data\\persons.csv")

    csvFileDF.show(20)

    //Modify the schema
    val csvMappedDF=csvFileDF.withColumnRenamed("First Name","first_name")
      .withColumnRenamed("Last Name","last_name")
      .withColumnRenamed("Date Of Birth","dob")
      .withColumnRenamed("Email","email")

    // Saving data to a JDBC source
    csvMappedDF.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "persons")
      .option("user", "postgres")
      .option("password", "postgres")
      .save()
  }
}
