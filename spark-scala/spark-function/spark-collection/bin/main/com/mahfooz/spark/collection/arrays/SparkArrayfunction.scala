package com.mahfooz.spark.collection.arrays

import org.apache.spark.sql.SparkSession

object SparkArrayfunction {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkArrayfunction")
      .master("local[*]")
      .getOrCreate()

    spark.sql("SELECT array(1, 2, 3)").show()

    spark.sql("SELECT array_contains(array(1, 2, 3), 2)").show()

    //array_distinct(array) - Removes duplicate values from the array.
    spark.sql("SELECT array_distinct(array(1, 2, 3, null, 3))").show()

    //array_except
    spark.sql(" SELECT array_except(array(1, 2, 3), array(1, 3, 5))").show()

    //array_intersect(array1, array2) - Returns an array of the elements in the intersection of array1
    //and array2, without duplicates.
    spark.sql("SELECT array_intersect(array(1, 2, 3), array(1, 3, 5))").show()

    //array_join(array, delimiter[, nullReplacement]) - Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.
    //If no value is set for nullReplacement, any null value is filtered.

    spark.stop()
  }
}
