package com.mahfooz.spark.checkpoint.util

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Calendar

object RecordGeneratorUtils {
  private val cal = Calendar.getInstance
  private val firstName = Array("John", "Kevin", "Lydia", "Nathan", "Jane", "Liz", "Sam", "Ruby", "Peter", "Rob", "Mahendra", "Noah", "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam", "Jean-Georges", "Holden", "Murthy", "Jonathan", "Jean", "Georges", "Oliver")
  private val lastName = Array("Smith", "Mills", "Perrin", "Foster", "Kumar", "Jones", "Tutt", "Main", "Haque", "Christie", "Karau", "Kahn", "Hahn", "Sanders")
  private val articles = Array("The", "My", "A", "Your", "Their")
  private val adjectives = Array("", "Great", "Beautiful", "Better", "Worse", "Gorgeous", "Terrific", "Terrible", "Natural", "Wild")
  private val nouns = Array("Life", "Trip", "Experience", "Work", "Job", "Beach")
  private val lang = Array("fr", "en", "es", "de", "it", "pt")
  private val daysInMonth = Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  def createDataframes(spark: SparkSession, recordCount: Int): Dataset[Row] = {
    println("-> createDataframes()")
    val schema = StructType(
      Array(
        StructField("name", StringType, false),
        StructField("title", StringType, false),
        StructField("rating", IntegerType, false),
        StructField("year", IntegerType, false),
        StructField("lang", StringType, false)
      )
    )
    var df: Dataset[Row] = null
    val inc = 500000L
    var recordCreated = 0L
    while (recordCreated < recordCount) {
      var recordInc = inc
      if (recordCreated + inc > recordCount)
        recordInc = recordCount - recordCreated
      var rows = Seq[Row]()
      for (j: Long <- 0L until recordInc) {
        rows = rows.+:( Row(
          getFirstName + " " + getLastName,
          getTitle,
          getRating,
          getRecentYears(25),
          getLang))
      }

      val rdd = spark.sparkContext.parallelize(rows)
      if (df == null)
        df = spark.createDataFrame(rdd, schema)
      else
        df = df.union(spark.createDataFrame(rdd, schema))
      recordCreated = df.count
      System.out.println(recordCreated + " records created")
    }
    df.show(3, false)
    System.out.println("<- createDataframe()")
    df
  }

  def getLang: String = lang(getRandomInt(lang.length))

  def getRecentYears(i: Int): Int = cal.get(Calendar.YEAR) - getRandomInt(i)

  def getRating: Int = getRandomInt(3) + 3

  def getRandomSSN: String = "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-" + getRandomInt(10) + getRandomInt(10) + "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)

  def getRandomInt(i: Int): Int = (Math.random * i).toInt

  def getFirstName: String = firstName(getRandomInt(firstName.length))

  def getLastName: String = lastName(getRandomInt(lastName.length))

  def getArticle: String = articles(getRandomInt(articles.length))

  def getAdjective: String = adjectives(getRandomInt(adjectives.length))

  def getNoun: String = nouns(getRandomInt(nouns.length))

  def getTitle: String = (getArticle + " " + getAdjective).trim + " " + getNoun

  def getIdentifier(identifiers: List[Int]): Int = {
    var i = 0
    do i = getRandomInt(60000) while ( {
      identifiers.contains(i)
    })
    i
  }

  def getLinkedIdentifier(linkedIdentifiers: List[Int]): Int = {
    if (linkedIdentifiers == null) return -1
    if (linkedIdentifiers.isEmpty) return -2
    val i = getRandomInt(linkedIdentifiers.size)
    linkedIdentifiers.apply(i)
  }

  def getLivingPersonDateOfBirth(format: String): String = {
    val year = cal.get(Calendar.YEAR) - getRandomInt(120) + 15
    val month = getRandomInt(12)
    val day = getRandomInt(daysInMonth(month)) + 1
    cal.set(year, month, day)
    val sdf = new SimpleDateFormat(format)
    sdf.format(cal.getTime)
  }
}