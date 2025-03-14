package com.mahfooz.dataset.join.joinWith

import com.mahfooz.spark.dataset.model.{Article, ArticleView, AuthorViews}
import org.apache.spark.sql.SparkSession

object JoinWithLeft {

  def main(args: Array[String]): Unit = {
    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JoinWithInner")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val articles = Seq(
      Article("Gergely", "Spark joins", 0),
      Article("Kris", "Athena with DataGrip", 1),
      Article("Kris", "Something Else", 2),
      Article("Gergely", "My article in preparation", 3)
    ).toDS

    val views = Seq(
      ArticleView(0, 1), //my article is not very popular :(
      ArticleView(1, 123), //Kris' article has been viewed 123 times
      ArticleView(2, 24), //Kris' not so popular article
      ArticleView(10, 104) //a deleted article
    ).toDS

    articles
      .joinWith(views,
        articles("id") === views("articleId"),
        "left")
      .map {
        case (a, null) => AuthorViews(a.author, 0)
        case (a, v) => AuthorViews(a.author, v.viewCount)
      }.show(false)

  }

}
