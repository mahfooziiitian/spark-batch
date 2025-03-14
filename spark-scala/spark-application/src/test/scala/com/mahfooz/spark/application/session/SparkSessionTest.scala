package com.mahfooz.spark.application.session

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkSessionTest extends AnyFunSuite with BeforeAndAfter {

  var sparkSession: SparkSession = _

  before{
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSessionTest")
      .getOrCreate()
  }

  after{
    sparkSession.stop()
  }

  test("test spark session should not be empty"){
    assert(sparkSession!=null)
  }

  test("test new spark session") {
    val newSparkSession = sparkSession.newSession()
    assert(sparkSession != newSparkSession)
  }

  test("spark session master details"){
    assert(sparkSession.sparkContext.master=="local[*]")
  }

  test("spark version") {
    assert(sparkSession.version=="3.3.2")
  }

}
