package com.mahfooz.spark.arango.model

import scala.beans.BeanProperty

case class Movie(@BeanProperty title: String) {
  def this() = this(title = null)
}
