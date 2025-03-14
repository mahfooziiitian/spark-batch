package com.mahfooz.spark.rdd.datasource.jdbc

import java.sql.{DriverManager, ResultSet}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcDataSources {


  def main(args: Array[String]): Unit = {

    //Define the Configuration
    val conf = new SparkConf();
    conf.setAppName("JdbcDataSources")
    conf.setMaster("local")

    //Define Context
    val ctx = new SparkContext(conf)

    //Define JDBC RDD
    val rdd = new JdbcRDD(ctx,() => {
      DriverManager.getConnection("jdbc:oracle:thin:@mttdevdbcl03a.intl.vsnl.co.in:1521/comtst",
        "move",
        "access123")},
      "SELECT EMP_ID,NAME FROM MOVE.EMP WHERE AGE > = ? AND AGE <= ?",
      0,
      40,
      1,
      (r: ResultSet) => { r.getInt(1); r.getString(2) } )
      .cache()
    //Print only first Column in the ResultSet
    System.out.println(rdd.first)
  }
}
