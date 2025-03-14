package com.mahfooz.spark.checkpoint

import com.mahfooz.spark.checkpoint.util.Mode.{CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER, NO_CACHE_NO_CHECKPOINT}
import com.mahfooz.spark.checkpoint.util.{Mode, RecordGeneratorUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CacheCheckpointApp {

  private var spark: SparkSession = null

  def main(args: Array[String]): Unit = {
    start()
  }

  private def start(): Unit = {
    spark = SparkSession.builder()
      .appName("Lab around cache and checkpoint")
      .master("local[*]")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "3g")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir(sys.env.getOrElse("DATA_HOME","data")+"\\spark\\checkpoint")

    val recordCount = 10000
    val t0 = processDataframe(recordCount, Mode.NO_CACHE_NO_CHECKPOINT)
    val t1 = processDataframe(recordCount, Mode.CACHE)
    val t2 = processDataframe(recordCount, Mode.CHECKPOINT)
    val t3 = processDataframe(recordCount, Mode.CHECKPOINT_NON_EAGER)

    System.out.println("\nProcessing times");
    System.out.println("Without cache ............... " + t0 + " ms");
    System.out.println("With cache .................. " + t1 + " ms");
    System.out.println("With checkpoint ............. " + t2 + " ms");
    System.out.println("With non-eager checkpoint ... " + t3 + " ms");
  }

  private def processDataframe(recordCount: Int, mode: Mode.Value): Long = {
    val df = RecordGeneratorUtils.createDataframes(this.spark, recordCount)
    val t0 = System.currentTimeMillis()
    var topDf = df.filter(col("rating").equalTo(5))
    mode match {
      case CACHE =>
        topDf = topDf.cache()
      case CHECKPOINT =>
        topDf = topDf.checkpoint()
      case CHECKPOINT_NON_EAGER =>
        topDf = topDf.checkpoint(false)
      case NO_CACHE_NO_CHECKPOINT =>
    }

    val langDf =
      topDf.groupBy("lang").count()
        .orderBy("lang").collect()

    val yearDf =
      topDf.groupBy("year").count()
        .orderBy(col("year").desc)
        .collect()

    val t1 = System.currentTimeMillis()

    println("Processing took " + (t1 - t0) + " ms.");
    println("Five-star publications per language")

    for (r <- langDf) {
      println(r.getString(0) + " ... " + r.getLong(1))
    }

    println("\nFive-star publications per year")
    for (r <- yearDf) {
      println(r.getInt(0) + " ... " + r.getLong(1))
    }
    t1 - t0
  }

}
