package in.kamranali.movies

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.{Codec, Source}
import org.apache.spark.sql.functions._

object PopularMoviesDataSets {

  def loadMovieNames() :Map[Int,String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames :Map[Int,String] = Map()

    var lines = Source.fromFile("./src/main/resources/moviedata/u.item").getLines()

    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames;

  }

  final case class Movie(movieID: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

    val lines = spark.sparkContext.textFile("./src/main/resources/moviedata/u.data")

    // Structuring the raw data
    var movies = lines.map(x => Movie(x.split("\t")(1).toInt))

    import spark.implicits._

    val moviesDS = movies.toDS()

    val topMovies = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()

    val top10 = topMovies.take(10)
    val names = loadMovieNames()

    for (result <- top10) {
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    spark.stop()

  }
}
