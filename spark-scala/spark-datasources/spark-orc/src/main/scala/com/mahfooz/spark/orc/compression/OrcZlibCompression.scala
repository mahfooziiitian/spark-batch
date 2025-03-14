/*
ORC stores data as columns and in compressed format hence it takes way less disk storage than other formats.
Spark supports the following compression options for ORC data source.

By default, it uses SNAPPY when not specified.

1. SNAPPY
2. ZLIB
3. LZO
4. NONE

 */
package com.mahfooz.spark.orc.compression

import org.apache.spark.sql.{SaveMode, SparkSession}

object OrcZlibCompression {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.getOrElse("DATA_HOME", "orc") + "/FileData/Orc"
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")

    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "")
    )
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    println(s"Orc implementation: ${spark.conf.get("spark.sql.orc.impl")}")

    val data = Seq(
      ("James ", "", "Smith", "36636", "M", 3000),
      ("Michael ", "Rose", "", "40288", "M", 4000),
      ("Robert ", "", "Williams", "42114", "M", 4000),
      ("Maria ", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "", "F", -1)
    )

    val columns =
      Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
    val df = spark.createDataFrame(data).toDF(columns: _*)
    df.printSchema()
    df.show(false)

    df.write.mode(SaveMode.Overwrite)
      .option("compression","zlib")
      .orc(s"${dataHome}/Compression/Zlib/employees.orc")
  }
}
