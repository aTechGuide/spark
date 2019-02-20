package in.kamranali.movies

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object PopularMovies {

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

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMoviesApp")

    // We want to broadCast ID -> MovieName map to entire cluster
    // Even though this data is very big but we are trying to avoid transmitting data more than once
    var nameDict = sc.broadcast(loadMovieNames())

    // Read the data file
    val lines = sc.textFile("./src/main/resources/moviedata/u.data")

    // Mapping to (movieID, 1) tuple
    var movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    var movieCounts = movies.reduceByKey((x,y) => x+ y)

    var flippedSortedMovies = movieCounts.map(x => (x._2, x._1)).sortByKey()

    var flippedSortedMoviesWithNames = flippedSortedMovies.map(x => (nameDict.value.get(x._2) , x._1 ))

    flippedSortedMoviesWithNames.collect().foreach(println)

  }
}
