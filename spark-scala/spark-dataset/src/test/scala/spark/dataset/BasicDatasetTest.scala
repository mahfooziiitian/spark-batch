package spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object BasicDatasetTest {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
    .master("local[*]")
    .appName("BasicDatasetTest")
    .getOrCreate()

    val pairs=List((33,"Will"),(26,"Jean-Luc"),
    (55,    "Hugh"),
    (26, "Deanna"),
    (26,   "Quark"),
    (55,  "Weyoun"),
    (33,  "Gowron"),
    (55,    "Will"),
    (26,  "Jadzia"),
    (27,   "Hugh"))

    val schema=new StructType(Array(
      StructField("age",IntegerType,false),
      StructField("name",StringType,false))
    )

    val dataRDD=spark.sparkContext.parallelize(pairs).map(record=>Row(record._1,record._2))

    val dataset=spark.createDataFrame(dataRDD,schema)

    val ageNameGroup=dataset.groupBy("age","name")
    .agg(max(length(col("name"))))
    .withColumnRenamed("max(length(name))","length")

    ageNameGroup.printSchema()

    val ageGroup=dataset.groupBy("age")
    .agg(max(length(col("name"))))
    .withColumnRenamed("max(length(name))","length")

    ageGroup.printSchema()

    ageGroup.createOrReplaceTempView("age_group")
    ageNameGroup.createOrReplaceTempView("age_name_group")

    spark.sql("select ag.age,ang.name from age_group as ag, age_name_group as ang " +
      "where ag.age=ang.age and ag.length=ang.length")
    .show()
  }
}
