package com.mahfooz.df.aggregation.groupby.max

import org.apache.spark.sql.SparkSession

case class Matrix (sp: String, mt: String, value: String,count:Int)

object FindMaxOtherFieldGroup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("find-max-other-field-group")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      Matrix("MM1",  "S1",     "a"  ,    3),
      Matrix("MM1",  "S1",     "n"  ,    2),
      Matrix("MM1",  "S3",     "cb"  ,   5),
      Matrix("MM2",  "S3",     "mk"  ,   8),
      Matrix("MM2",  "S4",     "bg"  ,   10),
      Matrix("MM2",  "S4",     "dgd"  ,   1),
      Matrix("MM4",  "S2",     "rd"  ,    2),
      Matrix("MM4",  "S2",     "cb"  ,    2),
      Matrix("MM4",  "S2",     "uyi"  ,   7)
    ).toDF

    //solution 1
    df.sort($"count".desc).dropDuplicates(Seq("sp","mt")).show(false)

    //solution 2
    //df.sort("count").groupBy("sp", "mt").tail(1)
  }

}
