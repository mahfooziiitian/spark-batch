package com.mahfooz.dataset.util

import com.mahfooz.spark.dataset.model.Person

object PersonData {

  def read(): Seq[Person] = {
    Seq(
      Person("Mahfooz",37),
      Person("Shaziya",33),
      Person("Hamdan",3),
      Person("Hadiya",3)
    )
  }
}
