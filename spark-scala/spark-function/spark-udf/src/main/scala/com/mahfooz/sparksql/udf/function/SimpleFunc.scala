package com.mahfooz.sparksql.udf.function

object SimpleFunc {

  // create a function to convert grade to letter grade
  def letterGrade(score: Int): String = {
    score match {
      case score if score > 100 => "Cheating"
      case score if score >= 90 => "A"
      case score if score >= 80 => "B"
      case score if score >= 70 => "C"
      case _                    => "F"
    }
  }
}
