package spark.dsl.count

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import com.mahfooz.spark.dsl.model.Movie
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll

class SparkDslCountTest extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkDslCountTest")
    .getOrCreate()

  test("count with column") {
    import spark.implicits._
    val moviesDF = Seq(
      Movie(null, null, 2018L),
      Movie("John Doe", "Awesome Movie", 2018L),
      Movie(null, "Awesome Movie", 2018L),
      Movie("Mary Jane", "Awesome Movie", 2018L)
    ).toDF

    val countMovieDF = moviesDF
      .select(
        count("actor_name"),
        count("movie_title"),
        count("produced_year"),
        count("*")
      )
      .collect()

    assert(countMovieDF.apply(0)._1 == 2)
    assert(countMovieDF.apply(0)._2 == 3)
    assert(countMovieDF.apply(0)._4 == 4)
  }
  override protected def afterAll(): Unit = spark.stop()
}
