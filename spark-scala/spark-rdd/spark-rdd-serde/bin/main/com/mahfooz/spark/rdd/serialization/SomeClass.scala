package com.mahfooz.spark.rdd.serialization

class SomeClass extends Serializable {

  var someValue = 0
  def setSomeValue(i:Int) = {
    someValue = i
    this
  }

}