package com.mahfooz.spark.rdd.accumulator.inbuilt

import org.apache.spark.util.AccumulatorV2

class ListAccumulator extends AccumulatorV2[String,String]{

  private var initialString = ""
  private var finalString: String = ""

  override def isZero: Boolean = initialString.isEmpty && finalString.isEmpty

  override def copy(): AccumulatorV2[String, String]  = {
    val newAcc = new ListAccumulator
    newAcc.initialString = this.initialString
    newAcc.finalString = this.finalString
    newAcc
  }

  override def reset(): Unit = { initialString = ""; finalString = "" }

  override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, String]): Unit = ???

  override def value: String = ???
}
