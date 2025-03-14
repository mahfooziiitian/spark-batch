/*

 */
package com.mahfooz.dataframe.join.types.outer.left

import com.mahfooz.dataframe.join.model.Dept
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object LeftOuterJoinNull {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("LeftOuterJoinNull")
      .master("local[*]")
      .getOrCreate()

    val employeeSchema =StructType(
      Array(
        StructField("name",StringType,false),
        StructField("dept_no",IntegerType,true)
      )
    )
    import spark.implicits._

      val employeeRdd=spark.sparkContext.parallelize(
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

    val employeeDF = spark.createDataFrame(employeeRdd,employeeSchema)

      val deptDF = Seq(
      Dept(31, "Sales"),
      Dept(33, "Engineering"),
      Dept(34, "Finance"),
      Dept(35, "Marketing")
    ).toDF

    val joinType = "left_outer"
    val joinExpression = 'dept_no === 'id

    // the join type can be either "left_outer" or "leftouter"
    employeeDF.join(deptDF, joinExpression, joinType).printSchema()
    /*
    SQL equivalent:
    SELECT * FROM employeeDF LEFT OUTER JOIN deptDF
    ON employeeDF.dept_no = deptDF.id
     */

    employeeDF.join(deptDF, joinExpression, joinType).show(false)

  }
}
