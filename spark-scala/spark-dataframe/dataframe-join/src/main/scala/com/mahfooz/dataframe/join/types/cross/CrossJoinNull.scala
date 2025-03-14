package com.mahfooz.dataframe.join.types.cross

import com.mahfooz.dataframe.join.model.Dept
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CrossJoinNull {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(CrossJoinNull.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employeeSchema = StructType(
      Array(
        StructField("name", StringType, nullable = false),
        StructField("dept_no", IntegerType, nullable = true)
      )
    )

    val employeeRdd = spark.sparkContext.parallelize(
      Seq(
        Row("John", 31),
        Row("Jeff", 33),
        Row("Mary", 33),
        Row("Mandy", 34),
        Row("Julie", 34),
        Row("Kurt", null),
        Row("Mahfooz", null)
      )
    )

    val employeeDF = spark.createDataFrame(employeeRdd, employeeSchema)

    val deptDF = Seq( Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF

    // using crossJoin transformation and display the count
    println(employeeDF.crossJoin(deptDF).count)

    employeeDF.crossJoin(deptDF).explain(true)
    employeeDF.crossJoin(deptDF).show(100,truncate = false)

  }

}
