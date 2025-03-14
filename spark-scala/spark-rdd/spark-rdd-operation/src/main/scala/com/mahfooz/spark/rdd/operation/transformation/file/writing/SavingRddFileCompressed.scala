package com.mahfooz.spark.rdd.operation.transformation.file.writing

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.SparkSession

object SavingRddFileCompressed {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SavingRddFileCompressed")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Text/"
    val data = sc.textFile(s"${dataPath}/text-book.txt", 8)
    val words = data.flatMap(lines => lines.split(" "))
    words.saveAsTextFile(s"${dataPath}/rddSavingBzip",classOf[BZip2Codec])
  }

}
