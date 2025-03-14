package com.mahfooz.spark.junit

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.funsuite.AnyFunSuite
import org.junit.Assert._
import org.junit.Test

@RunWith(classOf[JUnitRunner])
class SparkJunitTest extends AnyFunSuite {
  @Test
  def test1 = assertEquals("Hi", "Hi")
}
