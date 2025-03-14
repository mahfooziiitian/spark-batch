package com.mahfooz.spark.dataset.creation

import com.mahfooz.spark.dataset.model.Person
import org.apache.spark.sql.SparkSession


object DatasetFromSeq {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName(DatasetFromSeq.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val dsCaseClass = Seq(Person("John", 35)).toDS()
    dsCaseClass.show()
  }

}
