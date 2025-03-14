/*

Dataset can be created from DataFrame as well.
Call df.as[SomeCaseClass] to convert the DataFrame to a Dataset.

 */
package com.mahfooz.spark.dataset.creation

import org.apache.spark.sql.SparkSession
import com.mahfooz.spark.dataset.model.Person

object DatasetFromDF {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
    .builder()
    .appName(DatasetFromDF.getClass.getSimpleName)
    .master("local[*]")
    .getOrCreate()

    import spark.implicits._

    val peopleDS = spark.read.json("D:\\data\\spark\\json\\person.json").as[Person]
    peopleDS.show()

  }
}
