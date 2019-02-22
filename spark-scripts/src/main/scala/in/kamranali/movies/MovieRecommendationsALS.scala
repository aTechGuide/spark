package in.kamranali.movies

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation._

import scala.io.{Codec, Source}

object MovieRecommendationsALS {

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

    val sc = new SparkContext("local[*]", "MovieRecommendationsALSApp")

    /* Loading Movie Names */
    val nameDict = loadMovieNames()

    /* Loading Rating data */
    val data = sc.textFile("./src/main/resources/moviedata/u-als.data")

    // Data format (userID, movieID, rating, timestamp)
    val ratings = data.map(x => x.toString.split("\t")).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toInt)).cache()

    val rank=8
    val numIterations=20

    println("Training Recommendation model ...")
    val model = ALS.train(ratings, rank, numIterations)

    if (args.length > 0) {
      val userID = args(0).toInt

      println("Rating for UserID = " + userID + ":")

      val userRatings = ratings.filter(x => x.user == userID).collect().take(10)

      for (rating <- userRatings) {
          println(nameDict(rating.product.toInt) + ":" + rating.rating.toString)
      }

      println("Top 10 Recommendations: ")

      val recommendations = model.recommendProducts(userID, 10)
      for (recommendation <- recommendations) {
        println(nameDict(recommendation.product.toInt) + " score " + recommendation.rating)
      }
    }

  }

}
