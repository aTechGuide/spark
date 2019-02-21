package in.kamranali.movies

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}

object MovieSimilarities {

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def loadMovieNames() : Map[Int, String] = {

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

    return movieNames
  }

  def filterDuplicated(userRatings:UserRatingPair) :Boolean = {

    val rating1 = userRatings._2._1
    val rating2 = userRatings._2._2

    val movie1 = rating1._1
    val movie2 = rating2._1

    return movie1 < movie2


  }

  def makePairs(userRatings: UserRatingPair) = {

    val rating1 = userRatings._2._1
    val rating2 = userRatings._2._2

    ((rating1._1, rating2._1), (rating1._2, rating2._2))

  }

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MovieSimilaritiesApp")

    /* Loading Movie Names */
    val nameDict = loadMovieNames()

    /* Loading Rating data */
    val data = sc.textFile("./src/main/resources/moviedata/u.data")

    /* Map rating to Key/value pari: userID => (MovieID, Rating)*/
    // Data format (userID, movieID, rating, timestamp)
    val ratings = data.map(x => x.toString.split("\t")).map( x => (x(0).toInt, (x(1).toInt, (x(2).toDouble))))

    // ratings.take(10).foreach(println)
    // Calculate permutations of combination of movies that the use watch together
    // If use watched A,B,C we will get back, A-B, B-C, C-A, B-A, A-A, B-B, C-C ...
    // We just want every possible pair of movies so that we can further evaluate how these two movies are similar to each other
    val joinedRatings= ratings.join(ratings)

    /* Sample after Join
    (778,((94,2.0),(94,2.0)))
    (778,((94,2.0),(7,4.0)))
    (778,((94,2.0),(78,1.0)))
    (778,((94,2.0),(1273,3.0)))
    (778,((94,2.0),(265,4.0)))
     */

    /* Filtering our duplicate pairing i.e. A-A, A-B or B-A */
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicated)

    /* We will calculate (movie1, movie2) => (rating1, rating2) */
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    /* Collect all ratings for all the movie pair*/
    // (movie1, movie2) => (rating1, rating2), (rating1, rating2) .....
    // Sample: ((220,977),CompactBuffer((1.0,2.0), (3.0,4.0), (2.0,3.0), (5.0,3.0), (2.0,1.0), (4.0,1.0), (5.0,5.0), (2.0,2.0), (3.0,3.0)))
    val moviePairRatings = moviePairs.groupByKey()

    // we will use moviePariSimilarities RDD more than once so we are caching the results
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    // Now we have similarities between movies, Let's extract similarities for a movie that we care for

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      val movieID:Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above

      val filteredResults = moviePairSimilarities.filter( x =>
      {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
      }
      )

      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)

      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }









  }

}
